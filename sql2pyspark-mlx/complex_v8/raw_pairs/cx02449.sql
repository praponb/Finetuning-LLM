WITH cleaned AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN voltage >= 239.6 THEN 'A' WHEN voltage >= 223.0 THEN 'B' ELSE 'C' END AS voltage_tier,
        UPPER(TRIM(quality)) AS quality_clean,
        GREATEST(kwh, voltage) AS greatest_kwh_voltage
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 8 AND 10
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY quality, voltage_tier), 0), 4) AS voltage_share_of_quality_voltage_tier,
        LAG(greatest_kwh_voltage) OVER (PARTITION BY voltage_tier ORDER BY voltage ASC, id) AS prev_greatest_kwh_voltage
    FROM cleaned
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN voltage >= 232.0 THEN 'large' WHEN voltage >= 224.8 THEN 'medium' ELSE 'small' END AS voltage_tier_2
    FROM windowed
    WHERE read_at >= TIMESTAMP '2025-01-19 09:00:00'
)
SELECT
    voltage_tier_2,
    site_id,
    GROUPING_ID() AS gid,
    MAX(read_at) AS last_read_at
FROM derived
GROUP BY GROUPING SETS ((voltage_tier_2, site_id), (voltage_tier_2), ())
ORDER BY voltage_tier_2 NULLS LAST, site_id NULLS LAST
