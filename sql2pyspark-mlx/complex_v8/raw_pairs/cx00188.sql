WITH base AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE (MONTH(read_at) IN (1, 3) OR quality = 'OK')
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY quality), 2) AS kwh_vs_quality_avg,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh,
        NTH_VALUE(kwh, 2) OVER (ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        LAST_VALUE(read_at) IGNORE NULLS OVER (PARTITION BY quality, site_id ORDER BY kwh ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at,
        LAST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY quality, site_id ORDER BY kwh ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_kwh,
        MIN(voltage) OVER (PARTITION BY quality ORDER BY read_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_voltage_so_far,
        MAX(voltage) OVER (PARTITION BY quality) AS quality_max_voltage,
        SIZE(COLLECT_SET(site_id) OVER w_quality) AS distinct_site_id_in_quality
    FROM base
    WINDOW w_quality AS (PARTITION BY quality)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality, DATE_TRUNC('MONTH', last_known_read_at))) AS distinct_site_id_in_quality_last_known_read_at_m,
        ROUND(quality_max_voltage / NULLIF(SUM(quality_max_voltage) OVER w_site_id, 0), 4) AS quality_max_voltage_share_of_site_id,
        MAX(second_kwh) OVER (PARTITION BY quality, DATE_TRUNC('MONTH', last_known_read_at)) AS quality_last_known_read_at_month_max_second_kwh
    FROM ranked
    WINDOW w_site_id AS (PARTITION BY site_id)
),
per_group AS (
    -- Group-level totals
    SELECT
        distinct_site_id_in_quality_last_known_read_at_m,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_ym,
        CASE WHEN voltage >= 233.2 THEN 'A' WHEN voltage >= 218.4 THEN 'B' ELSE 'C' END AS voltage_tier,
        MIN(last_known_read_at) AS first_last_known_read_at,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        SUM(CASE WHEN quality IN ('OK', 'EST') THEN 1 ELSE 0 END) AS n_quality_in_ok_est,
        COUNT(*) AS row_count
    FROM windowed
    WHERE read_at >= TIMESTAMP '2025-01-25 11:00:00'
    GROUP BY distinct_site_id_in_quality_last_known_read_at_m, DATE_FORMAT(read_at, 'yyyy-MM'), CASE WHEN voltage >= 233.2 THEN 'A' WHEN voltage >= 218.4 THEN 'B' ELSE 'C' END
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        distinct_site_id_in_quality_last_known_read_at_m,
        read_at_ym,
        voltage_tier,
        row_count,
        ROUND(AVG(row_count) OVER (PARTITION BY n_quality_in_ok_est, voltage_tier ORDER BY first_last_known_read_at, distinct_site_id_in_quality_last_known_read_at_m, read_at_ym, voltage_tier ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS row_count_avg_7,
        LAG(first_last_known_read_at) OVER w_n_quality_in_ok_est_voltage_2 AS prev_first_last_known_read_at,
        RANK() OVER (PARTITION BY n_quality_in_ok_est, voltage_tier ORDER BY first_last_known_read_at, distinct_site_id_in_quality_last_known_read_at_m, read_at_ym, voltage_tier) AS first_last_known_read_at_rank,
        DENSE_RANK() OVER (PARTITION BY n_quality_in_ok_est, voltage_tier ORDER BY first_last_known_read_at) AS first_last_known_read_at_dense_rank
    FROM per_group
    WINDOW w_n_quality_in_ok_est_voltage_2 AS (PARTITION BY n_quality_in_ok_est, voltage_tier ORDER BY first_last_known_read_at, distinct_site_id_in_quality_last_known_read_at_m, read_at_ym, voltage_tier)
)
SELECT
    distinct_site_id_in_quality_last_known_read_at_m,
    read_at_ym,
    voltage_tier,
    row_count,
    row_count_avg_7,
    prev_first_last_known_read_at,
    first_last_known_read_at_dense_rank,
    LOWER(TRIM(read_at_ym)) AS read_at_ym_clean,
    GREATEST(row_count_avg_7, row_count) AS greatest_row_count_avg_7_row_count
FROM with_trends
WHERE first_last_known_read_at_rank > 2
ORDER BY voltage_tier, first_last_known_read_at_dense_rank
