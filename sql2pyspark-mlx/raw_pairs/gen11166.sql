WITH flattened AS (
    SELECT
        id,
        log_ts,
        level,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, v
)
SELECT
    attr_key,
    MIN(log_ts) AS first_seen,
    COUNT(*) AS num_rows,
    COUNT_IF(attr_key = 'region') AS region_events
FROM flattened
GROUP BY 1
HAVING COUNT(*) < 1000;
