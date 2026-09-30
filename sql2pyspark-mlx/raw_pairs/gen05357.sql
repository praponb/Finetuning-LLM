WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS attr_key, attr_value
)
-- Multi-level aggregation
SELECT
    attr_key,
    MAX(log_ts) AS last_seen,
    SORT_ARRAY(COLLECT_SET(attr_key)) AS attr_key_list,
    MAX(log_ts) AS last_seen_2
FROM exploded
GROUP BY attr_key
ORDER BY attr_key NULLS LAST;
