WITH flattened AS (SELECT id, service, host, level, k, attr_value FROM app_logs LATERAL VIEW EXPLODE(context) e AS k, attr_value) SELECT id, service, host, k, attr_value FROM flattened ORDER BY host
