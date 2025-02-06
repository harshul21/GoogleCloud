from flask import Flask, request, jsonify
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from datetime import datetime
import logging
from typing import Dict, Any

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# BigQuery configuration
PROJECT_ID = "your-project-id"
DATASET_ID = "analytics_dataset"
TABLE_ID = "tab_clicks"

class BigQueryClient:
    def __init__(self, project_id: str, credentials_path: str = None):
        """Initialize BigQuery client"""
        try:
            if credentials_path:
                credentials = service_account.Credentials.from_service_account_file(
                    credentials_path
                )
                self.client = bigquery.Client(credentials=credentials, project=project_id)
            else:
                self.client = bigquery.Client(project=project_id)
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {str(e)}")
            raise

    def load_data(self, data: pd.DataFrame, dataset_id: str, table_id: str) -> None:
        """Load data into BigQuery table"""
        try:
            table_ref = f"{PROJECT_ID}.{dataset_id}.{table_id}"
            
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=[
                    bigquery.SchemaField("user_id", "STRING"),
                    bigquery.SchemaField("tab_name", "STRING"),
                    bigquery.SchemaField("click_timestamp", "TIMESTAMP"),
                    bigquery.SchemaField("session_id", "STRING"),
                    bigquery.SchemaField("app_version", "STRING"),
                    bigquery.SchemaField("device_info", "STRING")
                ]
            )

            load_job = self.client.load_table_from_dataframe(
                data,
                table_ref,
                job_config=job_config
            )
            load_job.result()
            
            logger.info(f"Successfully loaded {len(data)} rows into {table_ref}")
        except Exception as e:
            logger.error(f"Failed to load data to BigQuery: {str(e)}")
            raise

# Initialize BigQuery client
bq_client = BigQueryClient(PROJECT_ID)

def validate_click_data(data: Dict[str, Any]) -> bool:
    """Validate the incoming click data"""
    required_fields = ['user_id', 'tab_name', 'session_id', 'app_version', 'device_info']
    return all(field in data for field in required_fields)

@app.route('/api/v1/track-click', methods=['POST'])
def track_click():
    try:
        data = request.get_json()
        
        if not validate_click_data(data):
            return jsonify({
                'error': 'Missing required fields'
            }), 400

        # Create DataFrame with received data
        df = pd.DataFrame([{
            'user_id': data['user_id'],
            'tab_name': data['tab_name'],
            'click_timestamp': datetime.utcnow(),
            'session_id': data['session_id'],
            'app_version': data['app_version'],
            'device_info': data['device_info']
        }])

        # Load to BigQuery
        bq_client.load_data(df, DATASET_ID, TABLE_ID)

        return jsonify({
            'status': 'success',
            'message': 'Click event recorded successfully'
        }), 200

    except Exception as e:
        logger.error(f"Error processing click event: {str(e)}")
        return jsonify({
            'error': 'Internal server error'
        }), 500

@app.route('/api/v1/user-clicks/<user_id>', methods=['GET'])
def get_user_clicks(user_id):
    try:
        query = f"""
        SELECT tab_name, COUNT(*) as click_count
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE user_id = @user_id
        GROUP BY tab_name
        ORDER BY click_count DESC
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_id", "STRING", user_id)
            ]
        )

        query_job = bq_client.client.query(query, job_config=job_config)
        results = query_job.result()

        click_stats = [{
            'tab_name': row.tab_name,
            'click_count': row.click_count
        } for row in results]

        return jsonify({
            'user_id': user_id,
            'click_statistics': click_stats
        }), 200

    except Exception as e:
        logger.error(f"Error retrieving click statistics: {str(e)}")
        return jsonify({
            'error': 'Internal server error'
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
