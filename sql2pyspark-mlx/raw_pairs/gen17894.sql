WITH lines AS (
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS k, v
),
base AS (
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        k,
        SUBSTRING(host, 1, 3) AS host_prefix,
        status_code + latency_ms AS status_code_plus_latency_ms
    FROM lines
    WHERE k IS DISTINCT FROM 'version'
),
metrics AS (
    SELECT
        id,
        latency_ms,
        status_code,
        host_prefix,
        status_code_plus_latency_ms,
        CUME_DIST() OVER (ORDER BY latency_ms DESC NULLS LAST) AS latency_ms_cume_dist,
        RANK() OVER (ORDER BY status_code) AS status_code_rank,
        MAX(status_code_plus_latency_ms) OVER (PARTITION BY host) AS host_max_status_code_plus_latency_ms,
        CUME_DIST() OVER (ORDER BY latency_ms) AS latency_ms_cume_dist_2
    FROM base
),
valid AS (
    SELECT
        id,
        status_code,
        host_prefix,
        status_code_plus_latency_ms,
        latency_ms_cume_dist,
        status_code_rank,
        host_max_status_code_plus_latency_ms,
        latency_ms_cume_dist_2,
        COALESCE(host_max_status_code_plus_latency_ms, 0) AS host_max_status_code_plus_latency_ms_filled
    FROM metrics
    WHERE latency_ms_cume_dist < 0.31
),
metrics_app_logs AS (
    SELECT
        latency_ms_cume_dist,
        host_max_status_code_plus_latency_ms_filled,
        RANK() OVER (ORDER BY host_max_status_code_plus_latency_ms DESC NULLS FIRST) AS host_max_status_code_plus_latency_ms_rank
    FROM valid
    WHERE latency_ms_cume_dist < 0.24
),
scored AS (
    SELECT
        latency_ms_cume_dist,
        host_max_status_code_plus_latency_ms_filled,
        RANK() OVER (ORDER BY host_max_status_code_plus_latency_ms_filled DESC NULLS FIRST) AS host_max_status_code_plus_latency_ms_filled_rank,
        DENSE_RANK() OVER (ORDER BY host_max_status_code_plus_latency_ms_rank DESC) AS host_max_status_code_plus_latency_ms_rank_dense_rank,
        DENSE_RANK() OVER (ORDER BY host_max_status_code_plus_latency_ms_filled DESC NULLS LAST) AS host_max_status_code_plus_latency_ms_filled_dense_rank,
        CUME_DIST() OVER w_part AS latency_ms_cume_dist_cume_dist,
        DENSE_RANK() OVER w_running AS host_max_status_code_plus_latency_ms_rank_dense_rank_2
    FROM metrics_app_logs
    WINDOW w_part AS (ORDER BY latency_ms_cume_dist DESC), w_running AS (ORDER BY host_max_status_code_plus_latency_ms_rank)
)
SELECT
    latency_ms_cume_dist,
    host_max_status_code_plus_latency_ms_filled,
    host_max_status_code_plus_latency_ms_filled_dense_rank,
    host_max_status_code_plus_latency_ms_rank_dense_rank_2
FROM scored
