WITH exploded AS (
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, v
),
flattened_attrs AS (
    SELECT
        status_code,
        v,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 8) != 0 AS INT) AS flag_bit8
    FROM exploded
),
metrics AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY flag_bit8 NULLS LAST) AS flag_bit8_dense_rank,
        PERCENT_RANK() OVER (ORDER BY status_code_high NULLS LAST) AS status_code_high_percent_rank,
        RANK() OVER (ORDER BY flag_bit8 DESC NULLS LAST) AS flag_bit8_rank,
        RANK() OVER (ORDER BY status_code_high NULLS LAST) AS status_code_high_rank
    FROM flattened_attrs
),
with_windows AS (
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY status_code_high_percent_rank DESC) AS status_code_high_percent_rank_percent_rank,
        PERCENT_RANK() OVER (ORDER BY status_code) AS status_code_percent_rank,
        DENSE_RANK() OVER w_running AS flag_bit8_dense_rank_2,
        DENSE_RANK() OVER w_recent AS status_code_high_dense_rank
    FROM metrics
    WINDOW w_running AS (ORDER BY flag_bit8 DESC NULLS FIRST), w_recent AS (ORDER BY status_code_high NULLS LAST)
),
valid AS (
    SELECT
        v,
        status_code_high,
        flag_bit8_dense_rank,
        status_code_high_percent_rank,
        flag_bit8_rank,
        status_code_high_rank,
        status_code_high_percent_rank_percent_rank,
        status_code_percent_rank,
        flag_bit8_dense_rank_2,
        status_code_high_dense_rank,
        COALESCE(status_code_high_percent_rank_percent_rank, 0) AS status_code_high_percent_rank_percent_rank_filled
    FROM with_windows
    WHERE status_code_high_percent_rank >= 0.41
),
with_windows_app_logs AS (
    SELECT
        v,
        status_code_high,
        flag_bit8_dense_rank,
        status_code_high_percent_rank,
        status_code_high_percent_rank_percent_rank,
        RANK() OVER (ORDER BY status_code_high_dense_rank) AS status_code_high_dense_rank_rank
    FROM valid
)
SELECT
    CASE WHEN status_code_high >= 3 THEN 'LARGE' ELSE 'SMALL' END AS status_code_high_tier,
    APPROX_PERCENTILE(status_code_high_percent_rank_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS status_code_high_percent_rank_percent_rank_quartiles,
    MIN(status_code_high_percent_rank) AS min_status_code_high_percent_rank,
    ROUND(AVG(flag_bit8_dense_rank), 2) AS avg_flag_bit8_dense_rank,
    PERCENTILE_APPROX(status_code_high_percent_rank_percent_rank, 0.95) AS status_code_high_percent_rank_percent_rank_p95,
    COUNT(*) AS event_count,
    COUNT(*) AS cnt
FROM with_windows_app_logs
GROUP BY status_code_high_tier
ORDER BY cnt;
