WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        latency_ms,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, v
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        level,
        latency_ms,
        k,
        v,
        COALESCE(k, 'Unknown') AS k_filled
    FROM exploded
    WHERE k = 'trace'
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT level, id
    FROM filtered
    INTERSECT ALL
    SELECT level, id
    FROM app_logs
    WHERE latency_ms >= 1426
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.level = k.level AND s.id = k.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY k), 4) AS latency_ms_share,
        RANK() OVER (ORDER BY latency_ms DESC) AS latency_ms_rank
    FROM reconciled
),
filtered_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LEAST(latency_ms_share, latency_ms) AS least_latency_ms_share_latency_ms,
        CONCAT_WS('/', level, k) AS level_k_key,
        COALESCE(k, 'N/A') AS k_filled_2
    FROM scored
    WHERE level = 'WARN'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(latency_ms_rank) OVER (PARTITION BY k, level) AS k_max_latency_ms_rank,
        SUM(latency_ms_rank) OVER (PARTITION BY level) AS level_sum_latency_ms_rank,
        PERCENT_RANK() OVER (ORDER BY latency_ms_rank DESC NULLS LAST) AS latency_ms_rank_percent_rank
    FROM filtered_app_logs
)
-- Multi-level aggregation
SELECT
    CASE WHEN latency_ms_share >= 0.61 THEN 'LARGE' ELSE 'SMALL' END AS latency_ms_share_tier,
    CASE WHEN least_latency_ms_share_latency_ms >= 85 THEN 'PREMIUM' ELSE 'STANDARD' END AS least_latency_ms_share_latency_ms_tier,
    SUM(CASE WHEN level = 'WARN' THEN latency_ms_rank ELSE 0 END) AS warn_latency_ms_rank,
    BIT_OR(latency_ms) AS latency_ms_bit_or,
    MAX(k_max_latency_ms_rank) FILTER (WHERE level = 'INFO') AS info_max_k_max_latency_ms_rank
FROM metrics
GROUP BY CUBE (CASE WHEN latency_ms_share >= 0.61 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN least_latency_ms_share_latency_ms >= 85 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING BIT_OR(latency_ms) > 1;
