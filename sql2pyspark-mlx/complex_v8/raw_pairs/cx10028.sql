WITH base_readings AS (
    -- Filter meter_readings
    SELECT *
    FROM meter_readings
    WHERE kwh BETWEEN 14.5 AND 110.5
      AND HOUR(read_at) BETWEEN 8 AND 14
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        ROW_NUMBER() OVER (PARTITION BY site_id, quality ORDER BY kwh ASC, id) AS seq_no,
        RANK() OVER (PARTITION BY site_id ORDER BY voltage DESC) AS voltage_rank,
        ROUND(PERCENT_RANK() OVER w_all_by_voltage, 3) AS voltage_percent_rank,
        NTH_VALUE(read_at, 2) OVER (PARTITION BY site_id, quality ORDER BY kwh ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        SUM(kwh) OVER (PARTITION BY site_id ORDER BY voltage DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS kwh_sum_1_prior,
        ROUND(AVG(voltage) OVER (PARTITION BY quality, site_id), 2) AS quality_site_id_avg_voltage,
        COUNT(*) OVER (PARTITION BY quality, site_id) AS rows_in_quality_site_id,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER w_all_by_voltage)) / 60, 1) AS minutes_since_prev_read_at
    FROM base_readings
    WINDOW w_all_by_voltage AS (ORDER BY voltage DESC, id), w_quality_site_id AS (PARTITION BY quality, site_id)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY voltage_rank, quality ORDER BY kwh DESC, id) AS kwh_quintile,
        SUM(voltage_percent_rank) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), DATE_TRUNC('MONTH', second_read_at)) AS read_at_month_second_read_at_month_sum_voltage_p,
        MAX(kwh) OVER (PARTITION BY voltage_rank ORDER BY kwh_sum_1_prior ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_kwh_so_far,
        ROUND(CUME_DIST() OVER w_voltage_rank_by_kwh_sum_1_pr_2, 3) AS kwh_sum_1_prior_cume_dist,
        NTH_VALUE(voltage_percent_rank, 2) OVER (PARTITION BY voltage_rank ORDER BY kwh_sum_1_prior ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage_percent_rank,
        FIRST_VALUE(read_at) OVER (ORDER BY kwh_sum_1_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_read_at,
        ROUND(CUME_DIST() OVER (ORDER BY kwh_sum_1_prior), 3) AS kwh_sum_1_prior_cume_dist_2
    FROM with_trends
    WINDOW w_voltage_rank_by_kwh_sum_1_pr_2 AS (PARTITION BY voltage_rank ORDER BY kwh_sum_1_prior ASC NULLS LAST)
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    quality,
    seq_no,
    second_read_at,
    kwh_sum_1_prior,
    quality_site_id_avg_voltage,
    rows_in_quality_site_id,
    minutes_since_prev_read_at,
    kwh_quintile,
    read_at_month_second_read_at_month_sum_voltage_p,
    max_kwh_so_far,
    kwh_sum_1_prior_cume_dist,
    second_voltage_percent_rank,
    first_read_at,
    GREATEST(max_kwh_so_far, kwh) AS greatest_max_kwh_so_far_kwh
FROM windowed
ORDER BY kwh_sum_1_prior NULLS LAST
