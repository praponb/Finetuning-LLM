WITH base AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh <= 81.3
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    voltage,
    quality,
    COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
    ABS(voltage - 233.2) AS voltage_distance
FROM base
ORDER BY voltage_distance NULLS LAST, voltage NULLS LAST, id
LIMIT 5
