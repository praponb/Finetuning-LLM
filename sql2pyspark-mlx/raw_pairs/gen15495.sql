WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, attr_value
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(level) AS level_lower,
        TRIM(level) AS clean_level,
        COALESCE(attr_key, 'N/A') AS attr_key_clean
    FROM flattened
    WHERE attr_key IS NULL
      AND attr_key IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        service,
        MIN(latency_ms) OVER w_ordered AS service_min_latency_ms,
        DENSE_RANK() OVER (ORDER BY latency_ms DESC) AS latency_ms_dense_rank,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service), 4) AS latency_ms_share,
        DENSE_RANK() OVER (PARTITION BY attr_key ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_dense_rank_2,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY level, host), 4) AS latency_ms_share_2
    FROM cleaned
    WHERE service = 'billing'
    WINDOW w_ordered AS (PARTITION BY service)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(latency_ms_dense_rank_2 / SUM(latency_ms_dense_rank_2) OVER (PARTITION BY service), 4) AS latency_ms_dense_rank_2_share,
        ROUND(latency_ms_dense_rank_2 / SUM(latency_ms_dense_rank_2) OVER (PARTITION BY service), 4) AS latency_ms_dense_rank_2_share_2,
        PERCENT_RANK() OVER (PARTITION BY service ORDER BY latency_ms_share_2 DESC NULLS FIRST) AS latency_ms_share_2_percent_rank,
        DENSE_RANK() OVER (PARTITION BY service ORDER BY latency_ms_dense_rank_2 NULLS LAST) AS latency_ms_dense_rank_2_dense_rank,
        RANK() OVER (PARTITION BY service ORDER BY service_min_latency_ms) AS service_min_latency_ms_rank
    FROM with_windows
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        service_min_latency_ms_rank / NULLIF(latency_ms_dense_rank_2_share, 0) AS service_min_latency_ms_rank_per_latency_ms_dense_rank_2_share,
        CASE WHEN service = 'checkout' THEN 1 ELSE 0 END AS is_checkout
    FROM metrics
    WHERE latency_ms_share_2_percent_rank < 0.57
)
SELECT
    id,
    service,
    service_min_latency_ms,
    latency_ms_share,
    latency_ms_dense_rank_2,
    latency_ms_dense_rank_2_share_2,
    latency_ms_dense_rank_2_dense_rank,
    service_min_latency_ms_rank_per_latency_ms_dense_rank_2_share
FROM filtered
ORDER BY latency_ms_dense_rank_2 DESC NULLS LAST, service_min_latency_ms
