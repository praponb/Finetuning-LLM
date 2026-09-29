WITH flattened AS (SELECT id, log_ts, level, k, value FROM app_logs LATERAL VIEW EXPLODE(context) e AS k, value) SELECT id, log_ts, k, value FROM flattened;
