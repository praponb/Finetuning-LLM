WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at
    FROM meter_readings
)
SELECT id, quality AS name
FROM recent
EXCEPT ALL
SELECT id, site_name
FROM sites
ORDER BY name, id
