WITH exploded AS (SELECT id, host, level, k, value FROM app_logs LATERAL VIEW OUTER EXPLODE(context) t AS k, value) SELECT host, k, value FROM exploded
