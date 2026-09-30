WITH cleaned_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        CASE WHEN voltage >= 239.6 THEN 'A' WHEN voltage >= 223.1 THEN 'B' ELSE 'C' END AS voltage_tier
    FROM meter_readings
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        voltage_tier,
        LAG(voltage) OVER (PARTITION BY quality ORDER BY kwh, id) AS prev_voltage,
        ROUND(CUME_DIST() OVER (PARTITION BY quality ORDER BY kwh), 3) AS kwh_cume_dist,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY voltage_tier), 2) AS kwh_vs_voltage_tier_avg,
        NTH_VALUE(kwh, 2) OVER (PARTITION BY quality ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh
    FROM cleaned_readings
)
-- Final projection
SELECT
    id,
    site_id,
    kwh_cume_dist,
    second_kwh
FROM metrics
ORDER BY id, site_id DESC
