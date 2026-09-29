WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, attr_value
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT *, PERCENT_RANK() OVER (ORDER BY latency_ms NULLS LAST) AS latency_ms_percent_rank
    FROM flattened
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        latency_ms,
        attr_value,
        PERCENT_RANK() OVER (PARTITION BY k, host ORDER BY latency_ms_percent_rank NULLS FIRST) AS latency_ms_percent_rank_percent_rank,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY k), 4) AS latency_ms_share
    FROM metrics
    WHERE latency_ms BETWEEN 1395 AND 1467
),
base AS (
    -- Keep recent, valid rows
    SELECT *, CEIL(latency_ms_share) AS latency_ms_share_whole
    FROM enriched
    WHERE latency_ms_percent_rank_percent_rank < 0.76
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        latency_ms,
        attr_value,
        latency_ms_percent_rank_percent_rank,
        latency_ms_share_whole,
        latency_ms * latency_ms_share_whole AS latency_ms_times_latency_ms_share_whole
    FROM base
    WHERE latency_ms BETWEEN 771 AND 1649
)
-- Grouped metrics
SELECT
    CASE WHEN latency_ms >= 2665 THEN 'PREMIUM' ELSE 'STANDARD' END AS latency_ms_tier,
    CASE WHEN latency_ms_percent_rank_percent_rank >= 0.6 THEN 'PREMIUM' ELSE 'STANDARD' END AS latency_ms_percent_rank_percent_rank_tier,
    PERCENTILE_APPROX(latency_ms, 0.9) AS latency_ms_p90,
    PERCENTILE_APPROX(latency_ms_percent_rank_percent_rank, 0.95) AS latency_ms_percent_rank_percent_rank_p95,
    PERCENTILE_APPROX(latency_ms_share_whole, 0.9) AS latency_ms_share_whole_p90,
    ROUND(AVG(latency_ms_times_latency_ms_share_whole), 2) AS avg_latency_ms_times_latency_ms_share_whole
FROM filtered
GROUP BY CASE WHEN latency_ms >= 2665 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN latency_ms_percent_rank_percent_rank >= 0.6 THEN 'PREMIUM' ELSE 'STANDARD' END
