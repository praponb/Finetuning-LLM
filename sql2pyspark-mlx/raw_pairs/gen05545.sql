WITH flattened AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, value
),
flattened_attrs AS (
    SELECT
        value,
        CAST((status_code & 2) != 0 AS INT) AS flag_bit2,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
),
metrics AS (
    SELECT *, RANK() OVER (ORDER BY status_code_high DESC NULLS FIRST) AS status_code_high_rank
    FROM flattened_attrs
),
metrics_app_logs AS (
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY status_code_high_rank DESC) AS status_code_high_rank_cume_dist,
        CUME_DIST() OVER (ORDER BY status_code_high NULLS FIRST) AS status_code_high_cume_dist,
        DENSE_RANK() OVER (ORDER BY status_code_high_rank DESC NULLS LAST) AS status_code_high_rank_dense_rank
    FROM metrics
),
with_windows AS (
    SELECT
        value,
        status_code_high,
        status_code_high_rank,
        status_code_high_rank_cume_dist,
        status_code_high_cume_dist,
        status_code_high_rank_dense_rank,
        CUME_DIST() OVER (ORDER BY flag_bit2 NULLS LAST) AS flag_bit2_cume_dist,
        PERCENT_RANK() OVER (ORDER BY status_code_high_rank_cume_dist DESC NULLS FIRST) AS status_code_high_rank_cume_dist_percent_rank
    FROM metrics_app_logs
)
SELECT
    CASE WHEN status_code_high_cume_dist >= 0.85 THEN 'LARGE' ELSE 'SMALL' END AS status_code_high_cume_dist_tier,
    APPROX_PERCENTILE(status_code_high_rank_cume_dist, 0.5) AS status_code_high_rank_cume_dist_p50,
    AVG(status_code_high_rank) AS avg_status_code_high_rank,
    PERCENTILE_APPROX(status_code_high_rank_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS status_code_high_rank_dense_rank_quartiles,
    ROUND(AVG(status_code_high_rank_cume_dist_percent_rank), 2) AS avg_status_code_high_rank_cume_dist_percent_rank,
    ROUND(AVG(status_code_high_rank_cume_dist), 2) AS avg_status_code_high_rank_cume_dist
FROM with_windows
GROUP BY CASE WHEN status_code_high_cume_dist >= 0.85 THEN 'LARGE' ELSE 'SMALL' END
HAVING avg_status_code_high_rank_cume_dist > 0
ORDER BY avg_status_code_high_rank_cume_dist_percent_rank DESC
