WITH scoped_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN voltage >= 239.6 THEN 'HIGH' WHEN voltage >= 223.3 THEN 'MEDIUM' ELSE 'LOW' END AS voltage_tier
    FROM meter_readings
)
SELECT
    'reading' AS origin,
    id,
    read_at AS happened_at,
    site_id
FROM scoped_readings
WHERE voltage <= 233.8
UNION ALL
SELECT
    'reading_2' AS origin,
    id,
    read_at AS happened_at,
    site_id
FROM meter_readings
WHERE site_id IS NOT NULL
