WITH flattened AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, v
)
SELECT
    key,
    SIZE(COLLECT_SET(key)) AS num_keys,
    COUNT(CASE WHEN service = 'billing' THEN 1 END) AS billing_count,
    SIZE(COLLECT_SET(key)) AS num_keys_2
FROM flattened
GROUP BY 1
HAVING num_keys_2 >= 1
