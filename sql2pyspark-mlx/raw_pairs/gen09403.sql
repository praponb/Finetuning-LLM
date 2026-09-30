WITH flattened AS (
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, attr_value
),
base AS (
    SELECT
        *,
        SUBSTRING(host, 1, 4) AS host_prefix,
        COALESCE(k, 'DIRECT') AS k_clean
    FROM flattened
    WHERE service NOT IN ('search', 'checkout', 'billing')
),
base_app_logs AS (
    SELECT
        *,
        INITCAP(TRIM(level)) AS level_title,
        COALESCE(k, 'OTHER') AS resolved_k
    FROM base
    WHERE k IS NOT NULL
      AND service = 'search'
),
filtered AS (
    SELECT *, status_code - latency_ms AS status_code_minus_latency_ms
    FROM base_app_logs
    WHERE level NOT IN ('INFO', 'DEBUG', 'ERROR')
),
filtered_app_logs AS (
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        attr_value,
        host_prefix,
        k_clean,
        level_title,
        status_code_minus_latency_ms,
        UPPER(service) AS service_upper
    FROM filtered
    WHERE k = 'region'
)
SELECT
    host,
    COUNT(CASE WHEN host = 'h4' THEN 1 END) AS h4_count,
    AVG(status_code) AS avg_status_code,
    STDDEV_SAMP(status_code) AS status_code_stddev,
    SIZE(COLLECT_SET(service)) AS num_services,
    ROUND(AVG(latency_ms), 2) AS avg_latency_ms,
    COUNT(*) FILTER (WHERE host = 'h2') AS h2_rows
FROM filtered_app_logs
GROUP BY host
