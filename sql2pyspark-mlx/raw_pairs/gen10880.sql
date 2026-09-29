WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, attr_value
)
SELECT
    id,
    log_ts,
    status_code,
    k,
    attr_value,
    CASE WHEN k RLIKE '^(trace|version)$' THEN 'GROUP_A' WHEN k RLIKE '^(region)$' THEN 'CORE' ELSE 'PRIORITY' END AS k_zone
FROM exploded
ORDER BY k_zone DESC NULLS LAST;
