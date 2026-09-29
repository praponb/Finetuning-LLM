WITH filtered AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh,
        LEAST(kwh, voltage) AS least_kwh_voltage,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh_2
    FROM meter_readings
    WHERE kwh > 50.4
      AND quality = 'BAD'
),
filtered_2 AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
      AND s.site_id IS NOT NULL
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        CASE WHEN least_kwh_voltage >= 208.2 THEN 'HIGH' WHEN least_kwh_voltage >= 49.0 THEN 'MEDIUM' ELSE 'LOW' END AS least_kwh_voltage_tier,
        COALESCE(least_kwh_voltage, 0.0) AS least_kwh_voltage_filled,
        COALESCE(voltage_per_kwh_2, 0.0) AS voltage_per_kwh_2_filled
    FROM filtered_2
    WHERE least_kwh_voltage >= 98.0
)
-- Subtotals with GROUPING SETS
SELECT
    site_id,
    COALESCE(least_kwh_voltage_tier, 'ALL') AS least_kwh_voltage_tier,
    GROUPING_ID() AS grouping_level,
    ROUND(SUM(least_kwh_voltage_filled) / NULLIF(SUM(voltage), 0), 3) AS least_kwh_voltage_filled_to_voltage_ratio
FROM enriched
GROUP BY GROUPING SETS ((site_id, least_kwh_voltage_tier), (site_id))
ORDER BY site_id NULLS LAST, least_kwh_voltage_tier NULLS LAST
