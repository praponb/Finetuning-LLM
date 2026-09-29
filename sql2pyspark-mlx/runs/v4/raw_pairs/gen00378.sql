WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        level,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, value
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        level,
        latency_ms,
        k,
        value,
        CONCAT_WS('-', k, level) k_level_key,
        UPPER(level) level_upper
    FROM lines
    WHERE latency_ms <= 1200
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(latency_ms) OVER (PARTITION BY k, level ORDER BY latency_ms, id, k ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) latency_ms_avg_3_prior,
        RANK() OVER (PARTITION BY k ORDER BY latency_ms DESC NULLS FIRST) latency_ms_rank,
        AVG(latency_ms) OVER (PARTITION BY level) level_avg_latency_ms
    FROM cleaned
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        level,
        k,
        value,
        level_upper,
        latency_ms_avg_3_prior,
        latency_ms_rank,
        level_avg_latency_ms,
        latency_ms_rank + latency_ms latency_ms_rank_plus_latency_ms,
        COALESCE(k, 'OTHER') resolved_k
    FROM enriched
    WHERE k IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER w_running latency_ms_avg_3_prior_bucket,
        AVG(latency_ms_rank_plus_latency_ms) OVER w_grp latency_ms_rank_plus_latency_ms_avg_3,
        LAG(latency_ms_avg_3_prior, 1, 0) OVER (PARTITION BY level ORDER BY latency_ms_rank_plus_latency_ms, id, k) prev_latency_ms_avg_3_prior
    FROM valid
    WINDOW w_running AS (PARTITION BY k ORDER BY latency_ms_avg_3_prior DESC, id, k), w_grp AS (PARTITION BY k, level ORDER BY latency_ms_avg_3_prior, id, k ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(level_avg_latency_ms, 2) OVER (ORDER BY prev_latency_ms_avg_3_prior, id, k) prev_level_avg_latency_ms,
        NTILE(4) OVER (PARTITION BY k ORDER BY prev_latency_ms_avg_3_prior, id, k) prev_latency_ms_avg_3_prior_bucket
    FROM metrics
)
-- Window metrics over several different partitions
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY k ORDER BY latency_ms_rank DESC, id, k) row_seq,
    ROW_NUMBER() OVER (ORDER BY latency_ms_avg_3_prior_bucket DESC, id, k) row_seq_2
FROM scored
