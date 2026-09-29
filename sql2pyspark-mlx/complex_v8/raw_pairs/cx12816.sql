WITH base_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        GREATEST(voltage, kwh) AS greatest_voltage_kwh,
        CASE WHEN voltage >= 229.1 THEN 'A' WHEN voltage >= 218.2 THEN 'B' ELSE 'C' END AS voltage_tier,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref
    FROM meter_readings
),
without_matches AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM base_readings s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.zone = 'Z4'
        )
)
SELECT *
FROM (
    SELECT site_id, voltage_tier, quality, kwh
    FROM without_matches
) src
PIVOT (
    SUM(kwh) AS amount, MAX(kwh) AS max_v
    FOR quality IN ('OK', 'EST', 'BAD')
)
