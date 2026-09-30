WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS k, v
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY host ORDER BY status_code) AS status_code_rank,
        CUME_DIST() OVER (PARTITION BY host ORDER BY status_code) AS status_code_cume_dist
    FROM flattened
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        status_code_rank / NULLIF(status_code_cume_dist, 0) AS status_code_rank_per_status_code_cume_dist,
        COALESCE(k, 'OTHER') AS k_filled
    FROM scored
    WHERE log_ts >= DATE '2025-06-01'
),
valid_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LEAST(latency_ms, status_code_cume_dist) AS least_latency_ms_status_code_cume_dist,
        COALESCE(k, 'NONE') AS resolved_k
    FROM valid
    WHERE log_ts > DATE '2025-06-01'
      AND k IS NOT NULL
      AND host = 'h3'
)
SELECT
    id,
    host,
    latency_ms,
    v,
    status_code_rank,
    status_code_cume_dist,
    status_code_rank_per_status_code_cume_dist,
    k_filled,
    least_latency_ms_status_code_cume_dist,
    CASE WHEN host RLIKE '^(h3|h4)$' THEN 'GROUP_A' WHEN host RLIKE '^(h1|h2)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS host_zone
FROM valid_app_logs
ORDER BY host_zone DESC
