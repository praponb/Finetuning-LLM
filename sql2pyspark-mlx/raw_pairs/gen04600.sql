WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, attr_value
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CORR(status_code, latency_ms) OVER (PARTITION BY host) AS corr_status_code_latency_ms,
        RANK() OVER w_part AS status_code_rank,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY key), 4) AS latency_ms_share,
        CORR(status_code, latency_ms) OVER (PARTITION BY key) AS corr_status_code_latency_ms_2,
        PERCENT_RANK() OVER w_recent AS latency_ms_percent_rank
    FROM flattened
    WINDOW w_part AS (ORDER BY status_code DESC), w_recent AS (ORDER BY latency_ms NULLS FIRST)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        latency_ms,
        status_code,
        key,
        attr_value,
        status_code_rank,
        latency_ms_share,
        corr_status_code_latency_ms_2,
        COALESCE(latency_ms_share, 0) AS latency_ms_share_filled,
        COALESCE(key, 'OTHER') AS key_filled
    FROM scored
    WHERE latency_ms <= 1061
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        status_code,
        key,
        attr_value,
        status_code_rank,
        latency_ms_share,
        corr_status_code_latency_ms_2,
        latency_ms_share_filled,
        key_filled,
        CASE WHEN latency_ms_share_filled >= 81 THEN 'High' WHEN latency_ms_share_filled >= 12 THEN 'Medium' ELSE 'Low' END AS latency_ms_share_filled_band
    FROM base
    WHERE key IS NOT NULL
)
-- Grouped metrics
SELECT
    CASE WHEN status_code_rank >= 8 THEN 'HIGH' ELSE 'LOW' END AS status_code_rank_tier,
    CASE WHEN latency_ms_share >= 0.65 THEN 'HIGH' ELSE 'LOW' END AS latency_ms_share_tier,
    ROUND(AVG(status_code), 2) AS avg_status_code,
    ROUND(AVG(latency_ms_share), 2) AS avg_latency_ms_share,
    RANK() OVER (PARTITION BY CASE WHEN status_code_rank >= 8 THEN 'HIGH' ELSE 'LOW' END ORDER BY ROUND(AVG(latency_ms_share), 2) DESC) AS avg_latency_ms_share_rank
FROM filtered
GROUP BY 1, 2
HAVING avg_status_code > 0;
