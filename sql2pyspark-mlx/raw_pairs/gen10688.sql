WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        latency_ms,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, v
)
SELECT
    log_ts,
    latency_ms,
    k,
    v,
    CASE WHEN k RLIKE '^(region|trace)$' THEN 'EXTENDED' ELSE 'CORE' END AS k_zone
FROM lines
ORDER BY k_zone DESC;
