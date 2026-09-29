SELECT id, host, status_code, key, value FROM app_logs LATERAL VIEW OUTER EXPLODE(context) li AS key, value
