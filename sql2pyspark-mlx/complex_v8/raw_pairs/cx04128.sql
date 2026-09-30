WITH recent_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE (HOUR(read_at) BETWEEN 10 AND 14 OR quality IN ('BAD', 'EST'))
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(kwh) OVER (ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh,
        ROUND(CUME_DIST() OVER (ORDER BY kwh DESC), 3) AS kwh_cume_dist,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY site_id, quality), 2) AS voltage_vs_site_id_quality_avg,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY site_id, quality), 2) AS kwh_vs_site_id_quality_avg,
        RANK() OVER w_all_by_voltage AS voltage_rank,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY quality), 0), 4) AS kwh_share_of_quality,
        DENSE_RANK() OVER w_quality_by_voltage AS voltage_dense_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY kwh DESC), 3) AS kwh_percent_rank,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id, quality), 2) AS site_id_quality_avg_voltage,
        voltage - LAG(voltage) OVER (ORDER BY kwh, id) AS voltage_change
    FROM recent_readings
    WINDOW w_all_by_voltage AS (ORDER BY voltage, id), w_site_id_quality AS (PARTITION BY site_id, quality), w_quality_by_voltage AS (PARTITION BY quality ORDER BY voltage DESC, id)
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        voltage,
        quality,
        running_kwh,
        kwh_cume_dist,
        voltage_rank,
        kwh_share_of_quality,
        voltage_dense_rank,
        site_id_quality_avg_voltage,
        voltage_change,
        GREATEST(kwh_share_of_quality, voltage_rank) AS greatest_kwh_share_of_quality_voltage_rank,
        CASE WHEN kwh_cume_dist >= 0.8 THEN 'large' WHEN kwh_cume_dist >= 0.5 THEN 'medium' ELSE 'small' END AS kwh_cume_dist_tier
    FROM sequenced
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(voltage) OVER (PARTITION BY voltage_dense_rank ORDER BY running_kwh DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS voltage_sum_6_prior,
        SUM(voltage_rank) OVER (ORDER BY greatest_kwh_share_of_quality_voltage_rank, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS voltage_rank_sum_2_prior,
        MAX(running_kwh) OVER w_kwh_cume_dist_tier AS kwh_cume_dist_tier_max_running_kwh,
        SUM(voltage_rank) OVER w_kwh_cume_dist_tier AS kwh_cume_dist_tier_sum_voltage_rank
    FROM tagged
    WINDOW w_kwh_cume_dist_tier AS (PARTITION BY kwh_cume_dist_tier)
)
SELECT
    id,
    voltage,
    quality,
    running_kwh,
    kwh_cume_dist,
    voltage_rank,
    voltage_dense_rank,
    site_id_quality_avg_voltage,
    kwh_cume_dist_tier,
    voltage_rank_sum_2_prior,
    kwh_cume_dist_tier_max_running_kwh
FROM metrics
ORDER BY quality, id
LIMIT 20
