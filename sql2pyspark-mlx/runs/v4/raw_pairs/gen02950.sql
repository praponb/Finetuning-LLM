WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, attr_value
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        level,
        status_code,
        attr_value,
        LOWER(attr_key) AS attr_key_lower
    FROM exploded
    WHERE level IN ('DEBUG', 'ERROR', 'WARN')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        service,
        level,
        status_code,
        attr_value,
        attr_key_lower,
        SUM(status_code) OVER (PARTITION BY level, service) AS level_sum_status_code,
        PERCENT_RANK() OVER (PARTITION BY service ORDER BY status_code) AS status_code_percent_rank
    FROM cleaned
),
daily AS (
    -- Grouped metrics
    SELECT
        level,
        CASE WHEN level_sum_status_code >= 1901 THEN 'LARGE' ELSE 'SMALL' END AS level_sum_status_code_tier,
        BIT_AND(status_code) AS status_code_bit_and,
        SUM(CASE WHEN service = 'auth' THEN status_code ELSE 0 END) AS auth_status_code,
        SIZE(COLLECT_SET(service)) AS num_services,
        APPROX_PERCENTILE(status_code, 0.95) AS status_code_p95,
        SORT_ARRAY(COLLECT_SET(service)) AS service_list
    FROM metrics
    GROUP BY level, level_sum_status_code_tier
)
SELECT
    level,
    level_sum_status_code_tier,
    status_code_p95,
    service_list
FROM daily
ORDER BY level_sum_status_code_tier NULLS LAST, status_code_p95;
