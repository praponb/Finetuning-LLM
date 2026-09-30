WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS k, value
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, SUBSTRING(k, 1, 2) AS k_prefix
    FROM flattened
    WHERE log_ts > TO_TIMESTAMP('2025-06-01 11:00:00')
      AND k IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        host,
        value,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY host, k), 4) AS status_code_share,
        MIN(latency_ms) OVER (PARTITION BY k) AS k_min_latency_ms
    FROM base
    WHERE service IN ('auth', 'checkout')
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM scored
    EXCEPT ALL
    SELECT id
    FROM app_logs
    WHERE service NOT IN ('search', 'checkout', 'auth')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN remaining_keys k ON s.id = k.id
)
-- Grouped metrics
SELECT
    CASE WHEN status_code_share >= 0.68 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_share_tier,
    SUM(CASE WHEN host = 'h4' THEN status_code_share ELSE 0 END) AS h4_status_code_share,
    ROUND(AVG(k_min_latency_ms), 2) AS avg_k_min_latency_ms,
    APPROX_PERCENTILE(status_code_share, ARRAY(0.25, 0.5, 0.75)) AS status_code_share_quartiles
FROM kept_rows
GROUP BY 1
ORDER BY avg_k_min_latency_ms DESC
