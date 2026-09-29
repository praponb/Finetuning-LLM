WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage < 239.6
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality, DATE_TRUNC('MONTH', read_at))) AS distinct_site_id_in_quality_read_at_month,
        voltage - LAG(voltage) OVER (PARTITION BY site_id ORDER BY voltage ASC NULLS LAST, id) AS voltage_change,
        ROUND(AVG(voltage) OVER (PARTITION BY quality, DATE_TRUNC('MONTH', read_at)), 2) AS quality_read_at_month_avg_voltage,
        DENSE_RANK() OVER (PARTITION BY quality ORDER BY read_at DESC, id) AS read_at_dense_rank,
        MAX(kwh) OVER (PARTITION BY site_id ORDER BY voltage ASC NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS kwh_max_4,
        MAX(kwh) OVER (PARTITION BY site_id ORDER BY voltage ASC NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS kwh_max_2,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY quality, site_id), 0), 4) AS kwh_share_of_quality_site_id,
        NTH_VALUE(voltage, 2) OVER (PARTITION BY site_id, quality ORDER BY kwh ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage,
        COUNT(*) OVER (PARTITION BY quality, site_id) AS rows_in_quality_site_id,
        SUM(voltage) OVER (PARTITION BY quality, DATE_TRUNC('MONTH', read_at)) AS quality_read_at_month_sum_voltage,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id, quality ORDER BY kwh ASC), 3) AS kwh_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY quality, site_id ORDER BY read_at DESC), 3) AS read_at_percent_rank,
        SUM(kwh) OVER (PARTITION BY site_id, quality ORDER BY kwh ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS kwh_sum_3_prior,
        NTH_VALUE(voltage, 2) OVER (PARTITION BY quality, site_id ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage_2,
        NTH_VALUE(kwh, 2) OVER (PARTITION BY site_id ORDER BY voltage ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        ROUND(voltage - AVG(voltage) OVER w_quality_site_id, 2) AS voltage_vs_quality_site_id_avg
    FROM scoped
    WINDOW w_quality_site_id AS (PARTITION BY quality, site_id)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        distinct_site_id_in_quality_read_at_month,
        quality_read_at_month_avg_voltage,
        read_at_dense_rank,
        kwh_max_4,
        kwh_max_2,
        kwh_share_of_quality_site_id,
        rows_in_quality_site_id,
        quality_read_at_month_sum_voltage,
        kwh_cume_dist,
        second_voltage_2,
        second_kwh,
        voltage_vs_quality_site_id_avg,
        DENSE_RANK() OVER (ORDER BY quality_read_at_month_sum_voltage DESC) AS quality_read_at_month_sum_voltage_dense_rank,
        ROW_NUMBER() OVER (ORDER BY quality_read_at_month_sum_voltage DESC, id) AS rn,
        ROUND(CUME_DIST() OVER (PARTITION BY read_at_dense_rank, distinct_site_id_in_quality_read_at_month ORDER BY kwh_max_4), 3) AS kwh_max_4_cume_dist,
        COUNT(*) OVER (PARTITION BY site_id, distinct_site_id_in_quality_read_at_month) AS rows_in_site_id_distinct_site_id_in_quality_read
    FROM windowed
)
-- Final projection
SELECT
    id,
    site_id,
    voltage,
    distinct_site_id_in_quality_read_at_month,
    quality_read_at_month_avg_voltage,
    read_at_dense_rank,
    kwh_max_4,
    kwh_share_of_quality_site_id,
    rows_in_quality_site_id,
    kwh_cume_dist,
    second_voltage_2,
    second_kwh,
    voltage_vs_quality_site_id_avg,
    quality_read_at_month_sum_voltage_dense_rank,
    rn,
    rows_in_site_id_distinct_site_id_in_quality_read
FROM with_windows
ORDER BY read_at_dense_rank NULLS LAST
