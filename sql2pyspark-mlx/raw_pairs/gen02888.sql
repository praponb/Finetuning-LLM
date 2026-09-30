WITH flattened AS (SELECT id, service, host, attr_key, attr_value FROM app_logs LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, attr_value) SELECT host, attr_key, attr_value FROM flattened;
