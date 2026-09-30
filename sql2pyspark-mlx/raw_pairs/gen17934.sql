WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, v
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY host, level ORDER BY status_code ASC) AS status_code_rank,
        SUM(status_code) OVER (PARTITION BY level ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_sum_2h,
        AVG(latency_ms) OVER w_recent AS latency_ms_avg_5_prior,
        COUNT(latency_ms) OVER w_ordered AS latency_ms_count_15m
    FROM lines
    WINDOW w_recent AS (PARTITION BY k, level ORDER BY latency_ms ASC, id ASC, k ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), w_ordered AS (PARTITION BY host ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 900 PRECEDING AND CURRENT ROW)
),
kept_keys AS (
    -- Set intersection on keys
    SELECT level, id
    FROM with_windows
    INTERSECT
    SELECT level, id
    FROM app_logs
    WHERE latency_ms BETWEEN 1376 AND 1953
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    JOIN kept_keys k ON s.level = k.level AND s.id = k.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        v,
        status_code_rank,
        status_code_sum_2h,
        latency_ms_avg_5_prior,
        latency_ms_count_15m,
        CASE WHEN status_code_rank >= 9 THEN 'High' WHEN status_code_rank >= 3 THEN 'Medium' ELSE 'Low' END AS status_code_rank_level,
        COALESCE(k, 'UNKNOWN') AS k_filled
    FROM reconciled
    WHERE k IS NOT NULL
      AND status_code BETWEEN 312 AND 416
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        latency_ms,
        status_code_rank,
        status_code_rank_level,
        CUME_DIST() OVER (PARTITION BY host ORDER BY latency_ms_avg_5_prior ASC NULLS LAST) AS latency_ms_avg_5_prior_cume_dist,
        MAX(latency_ms) OVER w_grp AS host_max_latency_ms,
        MAX(status_code_rank) OVER w_grp AS host_max_status_code_rank,
        ROUND(status_code_rank / SUM(status_code_rank) OVER (PARTITION BY host), 4) AS status_code_rank_share,
        RANK() OVER (ORDER BY latency_ms DESC) AS latency_ms_rank
    FROM base
    WHERE status_code_rank >= 3
    WINDOW w_grp AS (PARTITION BY host)
)
-- Running and moving aggregates
SELECT
    latency_ms,
    status_code_rank,
    status_code_rank_level,
    latency_ms_avg_5_prior_cume_dist,
    host_max_status_code_rank,
    status_code_rank_share,
    latency_ms_rank,
    DENSE_RANK() OVER (ORDER BY latency_ms_rank DESC NULLS LAST) AS latency_ms_rank_dense_rank,
    PERCENT_RANK() OVER (ORDER BY status_code_rank ASC NULLS FIRST) AS status_code_rank_percent_rank,
    RANK() OVER (ORDER BY latency_ms_rank DESC NULLS FIRST) AS latency_ms_rank_rank
FROM enriched
