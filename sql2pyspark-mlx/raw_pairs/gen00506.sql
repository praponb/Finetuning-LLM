WITH lines AS (
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, v
),
unpacked AS (
    SELECT
        status_code,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 8) != 0 AS INT) AS has_fault
    FROM lines
    WHERE status_code >= 363
),
scored AS (
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY status_code_high DESC) AS status_code_high_cume_dist,
        RANK() OVER (ORDER BY status_code) AS status_code_rank,
        PERCENT_RANK() OVER (ORDER BY status_code_high NULLS FIRST) AS status_code_high_percent_rank,
        RANK() OVER (ORDER BY status_code_high NULLS LAST) AS status_code_high_rank,
        DENSE_RANK() OVER (ORDER BY status_code_high NULLS LAST) AS status_code_high_dense_rank
    FROM unpacked
),
valid AS (
    SELECT
        status_code,
        status_code_high,
        has_fault,
        status_code_high_cume_dist,
        status_code_rank,
        status_code_high_dense_rank,
        COALESCE(status_code_high_dense_rank, 0) AS status_code_high_dense_rank_filled,
        status_code * status_code_high_percent_rank AS status_code_times_status_code_high_percent_rank
    FROM scored
    WHERE status_code_high_percent_rank > 0.22
),
filtered AS (
    SELECT *, FLOOR(status_code_times_status_code_high_percent_rank) AS status_code_times_status_code_high_percent_rank_whole
    FROM valid
    WHERE status_code_times_status_code_high_percent_rank <= 75
)
SELECT
    status_code,
    status_code_high,
    has_fault,
    status_code_high_cume_dist,
    status_code_rank,
    status_code_high_dense_rank,
    status_code_high_dense_rank_filled,
    status_code_times_status_code_high_percent_rank_whole
FROM filtered
