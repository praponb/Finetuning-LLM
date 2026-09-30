WITH lines AS (
    SELECT
        id,
        host,
        level,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, v
),
parsed AS (
    SELECT
        status_code,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 8) != 0 AS INT) AS flag_bit8
    FROM lines
    WHERE status_code BETWEEN 263 AND 416
),
with_windows AS (
    SELECT
        status_code,
        flag_bit8,
        RANK() OVER (ORDER BY status_code DESC) AS status_code_rank,
        DENSE_RANK() OVER w_recent AS flag_bit8_dense_rank,
        PERCENT_RANK() OVER w_recent AS flag_bit8_percent_rank,
        DENSE_RANK() OVER (ORDER BY flag_bit8 ASC NULLS FIRST) AS flag_bit8_dense_rank_2,
        RANK() OVER w_recent AS flag_bit8_rank
    FROM parsed
    WHERE status_code_high != 1
    WINDOW w_recent AS (ORDER BY flag_bit8 ASC NULLS LAST)
),
with_windows_app_logs AS (
    SELECT
        flag_bit8,
        flag_bit8_percent_rank,
        flag_bit8_rank,
        RANK() OVER w_hist AS status_code_rank_2
    FROM with_windows
    WINDOW w_hist AS (ORDER BY status_code ASC)
)
SELECT
    CASE WHEN flag_bit8_percent_rank >= 0.73 THEN 'HIGH' ELSE 'LOW' END AS flag_bit8_percent_rank_tier,
    ROUND(AVG(status_code_rank_2), 2) AS avg_status_code_rank_2,
    APPROX_PERCENTILE(flag_bit8, 0.95) AS flag_bit8_p95,
    PERCENTILE_APPROX(status_code_rank_2, 0.95) AS status_code_rank_2_p95,
    APPROX_PERCENTILE(flag_bit8_percent_rank, 0.5) AS flag_bit8_percent_rank_p50,
    COUNT(*) AS row_count
FROM with_windows_app_logs
GROUP BY CASE WHEN flag_bit8_percent_rank >= 0.73 THEN 'HIGH' ELSE 'LOW' END
ORDER BY avg_status_code_rank_2 DESC
