WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        FLOOR(kwh / 25) * 25 AS kwh_bucket
    FROM meter_readings
)
SELECT *
FROM (
    SELECT site_id, quality, kwh_bucket
    FROM scoped
) src
PIVOT (
    COUNT(kwh_bucket)
    FOR quality IN ('EST', 'BAD')
)
ORDER BY site_id
