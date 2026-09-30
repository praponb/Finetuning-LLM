WITH scoped_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        QUARTER(read_at) AS read_at_quarter
    FROM meter_readings
    WHERE site_id >= 9
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        voltage,
        read_at_quarter,
        MIN(read_at_quarter) OVER (PARTITION BY site_id, read_at_quarter) AS site_id_read_at_quarter_min_read_at_quarter,
        ROUND(CUME_DIST() OVER (PARTITION BY read_at_quarter ORDER BY read_at DESC), 3) AS read_at_cume_dist,
        MAX(read_at_quarter) OVER w_read_at_quarter AS read_at_quarter_max_read_at_quarter,
        MAX(voltage) OVER (PARTITION BY read_at_quarter ORDER BY read_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS voltage_max_7,
        MAX(voltage) OVER w_read_at_quarter AS read_at_quarter_max_voltage,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY read_at_quarter), 0), 4) AS kwh_share_of_read_at_quarter,
        SUM(voltage) OVER (ORDER BY read_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS voltage_sum_2_prior,
        COUNT(*) OVER (PARTITION BY read_at_quarter) AS rows_in_read_at_quarter,
        COUNT(*) OVER (PARTITION BY read_at_quarter ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAG(kwh) OVER (PARTITION BY TO_DATE(read_at), read_at_quarter ORDER BY read_at ASC, id) AS prev_kwh,
        ROUND(AVG(kwh) OVER (PARTITION BY TO_DATE(read_at), read_at_quarter ORDER BY read_at ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_7,
        kwh - LAG(kwh) OVER (PARTITION BY read_at_quarter ORDER BY read_at DESC, id) AS kwh_change,
        ROUND(PERCENT_RANK() OVER w_all_by_read_at_ties, 3) AS read_at_percent_rank
    FROM scoped_readings
    WINDOW w_read_at_quarter AS (PARTITION BY read_at_quarter), w_all_by_read_at_ties AS (ORDER BY read_at)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(read_at_percent_rank) OVER (PARTITION BY read_at_quarter ORDER BY kwh_change DESC, id) AS next_read_at_percent_rank,
        SUM(prev_kwh) OVER (PARTITION BY read_at_quarter ORDER BY kwh_change DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_kwh,
        MAX(kwh_share_of_read_at_quarter) OVER (PARTITION BY read_at_quarter) AS read_at_quarter_max_kwh_share_of_read_at_quarter
    FROM windowed
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        read_at_quarter,
        CASE WHEN kwh_avg_7 >= 84.0 THEN 'A' WHEN kwh_avg_7 >= 24.0 THEN 'B' ELSE 'C' END AS kwh_avg_7_tier,
        CASE WHEN voltage >= 237.5 THEN 'large' WHEN voltage >= 222.3 THEN 'medium' ELSE 'small' END AS voltage_tier,
        ROUND(SUM(site_id_read_at_quarter_min_read_at_quarter) / NULLIF(SUM(prev_kwh), 0), 3) AS site_id_read_at_quarter_min_read_at_quarter_to_p,
        ROUND(STDDEV(site_id_read_at_quarter_min_read_at_quarter), 2) AS stddev_site_id_read_at_quarter_min_read_at_quart
    FROM with_trends
    GROUP BY read_at_quarter, CASE WHEN kwh_avg_7 >= 84.0 THEN 'A' WHEN kwh_avg_7 >= 24.0 THEN 'B' ELSE 'C' END, CASE WHEN voltage >= 237.5 THEN 'large' WHEN voltage >= 222.3 THEN 'medium' ELSE 'small' END
)
SELECT
    read_at_quarter,
    kwh_avg_7_tier,
    site_id_read_at_quarter_min_read_at_quarter_to_p,
    stddev_site_id_read_at_quarter_min_read_at_quart
FROM rollup_base
ORDER BY stddev_site_id_read_at_quarter_min_read_at_quart DESC
