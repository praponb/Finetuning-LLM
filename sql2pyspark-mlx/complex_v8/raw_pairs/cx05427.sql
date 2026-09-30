WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
    WHERE kwh > 45.0
)
SELECT site_id AS entity_id
FROM base_readings
EXCEPT
SELECT id AS entity_id
FROM tariff_windows
ORDER BY entity_id DESC
