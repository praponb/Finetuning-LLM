WITH cleaned AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        CASE WHEN kwh >= 78.6 THEN 'large' WHEN kwh >= 35.0 THEN 'medium' ELSE 'small' END AS kwh_tier
    FROM meter_readings
    WHERE (site_id IS NOT NULL OR voltage <= 239.6)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        kwh_tier,
        ROUND(CUME_DIST() OVER (PARTITION BY kwh_tier, YEAR(read_at) ORDER BY voltage DESC), 3) AS voltage_cume_dist,
        kwh - LAG(kwh) OVER (PARTITION BY kwh_tier ORDER BY kwh ASC, id) AS kwh_change,
        MAX(voltage) OVER (PARTITION BY site_id, kwh_tier) AS site_id_kwh_tier_max_voltage,
        LAG(voltage) OVER (ORDER BY read_at, id) AS prev_voltage,
        SUM(kwh) OVER (ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh,
        ROUND(voltage / NULLIF(SUM(voltage) OVER w_site_id_kwh_tier, 0), 4) AS voltage_share_of_site_id_kwh_tier,
        LAG(kwh, 2) OVER (ORDER BY kwh DESC, id) AS prev2_kwh,
        ROUND(AVG(voltage) OVER (PARTITION BY kwh_tier), 2) AS kwh_tier_avg_voltage,
        MAX(voltage) OVER w_kwh_tier AS kwh_tier_max_voltage,
        ROUND(voltage - AVG(voltage) OVER w_kwh_tier, 2) AS voltage_vs_kwh_tier_avg,
        SUM(kwh) OVER (PARTITION BY kwh_tier) AS kwh_tier_sum_kwh,
        ROUND(PERCENT_RANK() OVER (PARTITION BY kwh_tier ORDER BY kwh ASC, id), 3) AS kwh_percent_rank,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY kwh_tier, site_id), 0), 4) AS voltage_share_of_kwh_tier_site_id,
        SUM(voltage) OVER (PARTITION BY kwh_tier, site_id) AS kwh_tier_site_id_sum_voltage,
        LEAD(voltage) OVER w_site_id_by_read_at AS next_voltage
    FROM cleaned
    WINDOW w_site_id_kwh_tier AS (PARTITION BY site_id, kwh_tier), w_kwh_tier AS (PARTITION BY kwh_tier), w_site_id_by_read_at AS (PARTITION BY site_id ORDER BY read_at, id)
),
summary AS (
    -- Summarise per group
    SELECT
        kwh_tier,
        site_id,
        CASE WHEN kwh_tier_avg_voltage >= 236.2 THEN 'A' WHEN kwh_tier_avg_voltage >= 225.8 THEN 'B' ELSE 'C' END AS kwh_tier_avg_voltage_tier,
        COUNT(*) AS row_count,
        ROUND(SUM(kwh_change) / NULLIF(SUM(running_kwh), 0), 3) AS kwh_change_to_running_kwh_ratio
    FROM metrics
    GROUP BY 1, 2, 3
)
-- Final projection
SELECT
    kwh_tier,
    site_id,
    row_count,
    kwh_change_to_running_kwh_ratio
FROM summary
