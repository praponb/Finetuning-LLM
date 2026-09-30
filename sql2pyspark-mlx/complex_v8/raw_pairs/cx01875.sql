WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        CASE WHEN kwh >= 106.0 THEN 'large' WHEN kwh >= 28.4 THEN 'medium' ELSE 'small' END AS kwh_tier,
        DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label
    FROM meter_readings
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(voltage) OVER (ORDER BY voltage, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_7,
        SUM(voltage) OVER (PARTITION BY kwh_tier, quality) AS kwh_tier_quality_sum_voltage,
        LAST_VALUE(voltage) IGNORE NULLS OVER (ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage,
        SIZE(COLLECT_SET(kwh_tier) OVER (PARTITION BY quality, site_id)) AS distinct_kwh_tier_in_quality_site_id,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY kwh_tier, quality)) AS distinct_site_id_in_kwh_tier_quality,
        NTILE(4) OVER (ORDER BY voltage, id) AS voltage_quartile,
        DENSE_RANK() OVER (ORDER BY voltage, id) AS voltage_dense_rank,
        NTH_VALUE(voltage, 2) OVER (PARTITION BY kwh_tier ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage
    FROM base_readings
)
SELECT
    id,
    read_at,
    voltage,
    quality,
    kwh_tier,
    read_at_label,
    voltage_avg_7,
    kwh_tier_quality_sum_voltage,
    last_known_voltage,
    distinct_site_id_in_kwh_tier_quality,
    voltage_dense_rank,
    second_voltage,
    GREATEST(voltage_avg_7, second_voltage) AS greatest_voltage_avg_7_second_voltage
FROM ranked
ORDER BY voltage_dense_rank NULLS LAST, last_known_voltage NULLS LAST
