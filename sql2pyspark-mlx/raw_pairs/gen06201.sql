WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, attr_value
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        log_ts,
        key,
        attr_value,
        LOWER(host) AS host_lower,
        COALESCE(key, 'N/A') AS resolved_key
    FROM exploded
    WHERE key IN ('region', 'trace')
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY log_ts DESC, id, key) AS latest_rank
    FROM valid
    QUALIFY latest_rank = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        key,
        attr_value,
        host_lower,
        latest_rank,
        ROW_NUMBER() OVER (PARTITION BY key ORDER BY latest_rank, id, key) AS row_seq,
        FIRST_VALUE(key, TRUE) OVER (ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_key
    FROM latest
),
scored_app_logs AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(row_seq) OVER (PARTITION BY key ORDER BY latest_rank, id, key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_row_seq,
        AVG(row_seq) OVER (PARTITION BY key ORDER BY latest_rank, id, key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_row_seq,
        MAX(row_seq) OVER (PARTITION BY key ORDER BY latest_rank, id, key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_row_seq,
        LAG(row_seq, 1) OVER (ORDER BY latest_rank, id, key) AS prev_row_seq
    FROM scored
    WHERE row_seq IS NULL
)
-- Grouped metrics
SELECT
    key,
    CASE WHEN prev_row_seq >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_row_seq_tier,
    SORT_ARRAY(COLLECT_SET(key)) AS key_list,
    SUM(row_seq) FILTER (WHERE key = 'version') AS version_total_row_seq,
    PERCENTILE_APPROX(last3_count_row_seq, 0.95) AS last3_count_row_seq_p95,
    SIZE(COLLECT_SET(key)) AS num_keys,
    SUM(CASE WHEN key = 'region' THEN last3_count_row_seq ELSE 0 END) AS region_last3_count_row_seq
FROM scored_app_logs
GROUP BY key, prev_row_seq_tier;
