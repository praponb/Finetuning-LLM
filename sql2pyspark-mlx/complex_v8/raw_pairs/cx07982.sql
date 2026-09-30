WITH scoped_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        TO_DATE(read_at) AS read_at_date
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-02-12 14:00:00'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(kwh) OVER (PARTITION BY quality, site_id) AS quality_site_id_sum_kwh,
        DENSE_RANK() OVER (ORDER BY voltage DESC, id) AS voltage_dense_rank,
        DENSE_RANK() OVER (ORDER BY kwh) AS kwh_dense_rank,
        LAG(read_at, 3) OVER w_all_by_voltage AS prev3_read_at,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY quality, site_id), 2) AS kwh_vs_quality_site_id_avg,
        LAST_VALUE(voltage) OVER (ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_voltage,
        ROUND(CUME_DIST() OVER (ORDER BY voltage DESC, id), 3) AS voltage_cume_dist,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY site_id, quality ORDER BY kwh ASC NULLS LAST, id))) / 60, 1) AS minutes_since_prev_read_at,
        FIRST_VALUE(read_at) IGNORE NULLS OVER (ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_read_at,
        ROUND(CUME_DIST() OVER (ORDER BY kwh, id), 3) AS kwh_cume_dist,
        RANK() OVER w_all_by_kwh_ties AS kwh_rank,
        ROW_NUMBER() OVER (ORDER BY voltage DESC, id) AS seq_no,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY quality, site_id), 2) AS voltage_vs_quality_site_id_avg,
        LAG(read_at) OVER w_all_by_voltage AS prev_read_at,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (ORDER BY voltage DESC, id))) / 60, 1) AS minutes_since_prev_read_at_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id, quality ORDER BY kwh ASC NULLS LAST, id), 3) AS kwh_percent_rank
    FROM scoped_readings
    WINDOW w_all_by_voltage AS (ORDER BY voltage DESC, id), w_all_by_kwh_ties AS (ORDER BY kwh)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        read_at_date,
        quality_site_id_sum_kwh,
        prev3_read_at,
        kwh_vs_quality_site_id_avg,
        last_voltage,
        first_known_read_at,
        kwh_cume_dist,
        kwh_rank,
        seq_no,
        voltage_vs_quality_site_id_avg,
        prev_read_at,
        minutes_since_prev_read_at_2,
        LAST_VALUE(kwh_rank) OVER (PARTITION BY site_id, kwh_rank ORDER BY kwh_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh_rank,
        LEAD(kwh_vs_quality_site_id_avg) OVER (PARTITION BY DATE_TRUNC('MONTH', prev3_read_at), seq_no ORDER BY kwh DESC, id) AS next_kwh_vs_quality_site_id_avg,
        MIN(kwh_dense_rank) OVER (PARTITION BY site_id, YEAR(first_known_read_at)) AS site_id_first_known_read_at_year_min_kwh_dense_r,
        RANK() OVER (PARTITION BY kwh_rank ORDER BY voltage DESC) AS voltage_rank,
        NTH_VALUE(quality_site_id_sum_kwh, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', prev3_read_at), seq_no ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_quality_site_id_sum_kwh,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', read_at_date), voltage_dense_rank ORDER BY voltage_cume_dist DESC NULLS FIRST, id) AS rn,
        NTILE(5) OVER (PARTITION BY site_id, kwh_rank ORDER BY kwh_cume_dist, id) AS kwh_cume_dist_quintile,
        COUNT(*) OVER (PARTITION BY YEAR(prev_read_at), kwh_dense_rank) AS rows_in_prev_read_at_year_kwh_dense_rank
    FROM sequenced
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    read_at_date,
    quality_site_id_sum_kwh,
    prev3_read_at,
    kwh_vs_quality_site_id_avg,
    last_voltage,
    kwh_cume_dist,
    kwh_rank,
    seq_no,
    voltage_vs_quality_site_id_avg,
    prev_read_at,
    minutes_since_prev_read_at_2,
    next_kwh_vs_quality_site_id_avg,
    site_id_first_known_read_at_year_min_kwh_dense_r,
    voltage_rank,
    rn,
    CASE WHEN kwh_cume_dist >= 0.6 THEN 'large' WHEN kwh_cume_dist >= 0.4 THEN 'medium' ELSE 'small' END AS kwh_cume_dist_tier,
    CONCAT(quality, '#', CAST(voltage_rank AS STRING)) AS quality_ref
FROM metrics
ORDER BY kwh_cume_dist DESC
