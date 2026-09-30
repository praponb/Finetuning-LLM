WITH prepared_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE MONTH(read_at) IN (2, 3)
)
SELECT id
FROM prepared_readings
EXCEPT
SELECT site_id AS id
FROM meter_readings
ORDER BY id
