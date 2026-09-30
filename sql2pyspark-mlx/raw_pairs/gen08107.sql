WITH exploded AS (
    SELECT
        id,
        host,
        latency_ms,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, v
),
valid AS (
    SELECT
        id,
        latency_ms,
        k,
        v,
        INITCAP(TRIM(host)) AS host_title,
        COALESCE(k, 'NONE') AS k_clean
    FROM exploded
    WHERE k IS NOT NULL
      AND k IS DISTINCT FROM 'trace'
),
filtered AS (
    SELECT
        id,
        latency_ms,
        k,
        host_title,
        k_clean,
        CASE WHEN k = 'trace' THEN 1 ELSE 0 END AS is_trace
    FROM valid
    WHERE k IS NOT NULL
      AND k IN ('region', 'version')
),
kept_keys AS (
    SELECT id
    FROM filtered
    EXCEPT DISTINCT
    SELECT id
    FROM app_logs
    WHERE TO_DATE(log_ts) < '2023-10-01'
),
reconciled AS (
    SELECT s.*
    FROM filtered s
    JOIN kept_keys k ON s.id = k.id
),
metrics AS (
    SELECT
        id,
        latency_ms,
        host_title,
        k_clean,
        is_trace,
        CUME_DIST() OVER (PARTITION BY k ORDER BY is_trace DESC NULLS LAST) AS is_trace_cume_dist,
        RANK() OVER (PARTITION BY k ORDER BY latency_ms) AS latency_ms_rank,
        DENSE_RANK() OVER (ORDER BY latency_ms DESC) AS latency_ms_dense_rank,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY k), 4) AS latency_ms_share
    FROM reconciled
    WHERE k NOT IN ('trace', 'version')
),
enriched AS (
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY latency_ms NULLS FIRST) AS latency_ms_percent_rank,
        DENSE_RANK() OVER w_grp AS is_trace_dense_rank,
        PERCENT_RANK() OVER w_grp AS is_trace_percent_rank
    FROM metrics
    WINDOW w_grp AS (ORDER BY is_trace DESC)
),
cleaned AS (
    SELECT
        id,
        latency_ms,
        k_clean,
        is_trace_cume_dist,
        latency_ms_rank,
        latency_ms_dense_rank,
        latency_ms_percent_rank,
        is_trace_dense_rank,
        is_trace_percent_rank,
        COALESCE(is_trace_cume_dist, 0) AS is_trace_cume_dist_filled
    FROM enriched
    WHERE is_trace = 26
)
SELECT
    CASE WHEN is_trace_dense_rank >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_trace_dense_rank_tier,
    COUNT(*) AS cnt,
    COUNT(DISTINCT id) AS distinct_ids
FROM cleaned
GROUP BY CASE WHEN is_trace_dense_rank >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END
