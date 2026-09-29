WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, value
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(k, 1, 3) AS k_prefix,
        UPPER(k) AS k_upper
    FROM flattened
    WHERE k IS NOT NULL
      AND k IN ('trace', 'region')
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(latency_ms) OVER (PARTITION BY service, k) AS service_min_latency_ms,
        AVG(status_code) OVER (PARTITION BY service ORDER BY status_code, id, k ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS status_code_avg_2_prior,
        LAG(status_code, 1, 0) OVER (ORDER BY status_code, id, k) AS prev_status_code,
        SUM(status_code) OVER (PARTITION BY k ORDER BY status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_code,
        COUNT(latency_ms) OVER (PARTITION BY k) AS k_count_latency_ms
    FROM filtered
)
SELECT
    id,
    service,
    status_code,
    k,
    value,
    k_upper,
    service_min_latency_ms,
    status_code_avg_2_prior,
    cumulative_status_code,
    CASE WHEN service RLIKE '^(search|auth)$' THEN 'CORE' WHEN service RLIKE '^(billing|checkout)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS service_zone
FROM scored
