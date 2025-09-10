import requests
import time
import notebookutils
import json

def get_sql_endpoint_for_lakehouse(workspace_id, lakehouse_name, access_token):
    """Get the SQL endpoint ID for a specific lakehouse"""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    # First, find the lakehouse
    lakehouses_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses"
    print(f"Looking for lakehouse: {lakehouse_name}")
    
    response = requests.get(lakehouses_url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to get lakehouses: {response.status_code} - {response.text}")
    
    lakehouses = response.json().get('value', [])
    target_lakehouse = None
    
    for lakehouse in lakehouses:
        if lakehouse['displayName'] == lakehouse_name:
            target_lakehouse = lakehouse
            break
    
    if not target_lakehouse:
        available_names = [lh['displayName'] for lh in lakehouses]
        raise Exception(f"Lakehouse '{lakehouse_name}' not found. Available: {available_names}")
    
    lakehouse_id = target_lakehouse['id']
    print(f"Found lakehouse '{lakehouse_name}' with ID: {lakehouse_id}")
    
    # Now find the SQL endpoint - it should be a separate item
    # List all SQL endpoints in the workspace
    sqlendpoints_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/sqlEndpoints"
    print("Looking for SQL endpoints in workspace...")
    
    response = requests.get(sqlendpoints_url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to get SQL endpoints directly: {response.status_code} - {response.text}")
        print("Trying alternative approach - listing all items...")
        
        # Alternative: List all items and filter for SQL endpoints
        items_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
        response = requests.get(items_url, headers=headers)
        
        if response.status_code != 200:
            raise Exception(f"Failed to get workspace items: {response.status_code} - {response.text}")
        
        items = response.json().get('value', [])
        sql_endpoints = [item for item in items if item.get('type') == 'SQLEndpoint']
        
        print(f"Found {len(sql_endpoints)} SQL endpoints via items API:")
        for endpoint in sql_endpoints:
            print(f"  - {endpoint['displayName']} (ID: {endpoint['id']})")
        
        # Look for SQL endpoint related to our lakehouse
        # SQL endpoints typically have names like "lakehouse_name - SQL analytics endpoint"
        target_sql_endpoint = None
        for endpoint in sql_endpoints:
            endpoint_name = endpoint['displayName'].lower()
            if (lakehouse_name.lower() in endpoint_name and 
                ('sql' in endpoint_name or 'analytics' in endpoint_name)):
                target_sql_endpoint = endpoint
                break
        
        if not target_sql_endpoint:
            # Try exact lakehouse name match
            for endpoint in sql_endpoints:
                if endpoint['displayName'].lower() == lakehouse_name.lower():
                    target_sql_endpoint = endpoint
                    break
        
        if not target_sql_endpoint:
            endpoint_names = [ep['displayName'] for ep in sql_endpoints]
            raise Exception(f"No SQL endpoint found for lakehouse '{lakehouse_name}'. Available SQL endpoints: {endpoint_names}")
        
        sql_endpoint_id = target_sql_endpoint['id']
        print(f"Found SQL endpoint: '{target_sql_endpoint['displayName']}' with ID: {sql_endpoint_id}")
        
    else:
        # Direct SQL endpoints API worked
        sql_endpoints = response.json().get('value', [])
        print(f"Found {len(sql_endpoints)} SQL endpoints:")
        
        for endpoint in sql_endpoints:
            print(f"  - {endpoint['displayName']} (ID: {endpoint['id']})")
        
        # Look for SQL endpoint related to our lakehouse
        target_sql_endpoint = None
        for endpoint in sql_endpoints:
            endpoint_name = endpoint['displayName'].lower()
            if (lakehouse_name.lower() in endpoint_name or 
                endpoint['displayName'].lower() == lakehouse_name.lower()):
                target_sql_endpoint = endpoint
                break
        
        if not target_sql_endpoint:
            endpoint_names = [ep['displayName'] for ep in sql_endpoints]
            raise Exception(f"No SQL endpoint found for lakehouse '{lakehouse_name}'. Available: {endpoint_names}")
        
        sql_endpoint_id = target_sql_endpoint['id']
        print(f"Found SQL endpoint: '{target_sql_endpoint['displayName']}' with ID: {sql_endpoint_id}")
    
    return sql_endpoint_id

def refresh_sql_endpoint_and_wait(lakehouse_name, workspace_id, max_wait_minutes=30):
    """Refresh SQL endpoint metadata for a lakehouse by name and wait for completion"""
    
    access_token = notebookutils.credentials.getToken("pbi")
    print(f"Using workspace ID: {workspace_id}")
    
    # Get the correct SQL endpoint ID
    sql_endpoint_id = get_sql_endpoint_for_lakehouse(workspace_id, lakehouse_name, access_token)
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    # Try the refresh with different request body options
    refresh_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/sqlEndpoints/{sql_endpoint_id}/refreshMetadata"
    print(f"Refresh URL: {refresh_url}")
    
    # Try empty body first (some APIs don't need parameters)
    response = requests.post(refresh_url, headers=headers, json={})
    
    print(f"Response status: {response.status_code}")
    print(f"Response: {response.text}")
    
    if response.status_code == 202:
        status_url = response.headers.get('Location')
        if not status_url:
            print("⚠️ No Location header, assuming immediate completion")
            return True
        
        print(f"Refresh started. Monitoring status...")
        
        # Poll for completion
        max_wait_seconds = max_wait_minutes * 60
        start_time = time.time()
        
        while time.time() - start_time < max_wait_seconds:
            status_response = requests.get(status_url, headers=headers)
            
            if status_response.status_code == 200:
                print(f"✅ Refresh completed successfully!")
                return True
            elif status_response.status_code == 202:
                print("⏳ Refresh still in progress...")
                time.sleep(30)
            else:
                raise Exception(f"Status check failed: {status_response.status_code} - {status_response.text}")
        
        raise Exception(f"Refresh timed out after {max_wait_minutes} minutes")
    
    elif response.status_code == 200:
        print("✅ Refresh completed immediately!")
        return True
    else:
        raise Exception(f"Refresh failed to start: {response.status_code} - {response.text}")

# Usage
try:
    workspace_id = ""
    lakehouse_name = "lakehouse_main"
    
    refresh_sql_endpoint_and_wait(
        lakehouse_name=lakehouse_name,
        workspace_id=workspace_id,
        max_wait_minutes=30
    )
    print("Pipeline can continue - SQL endpoint is refreshed!")
except Exception as e:
    print(f"Error: {e}")
    raise
