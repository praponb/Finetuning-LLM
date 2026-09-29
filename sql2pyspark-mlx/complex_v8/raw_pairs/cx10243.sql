WITH cleaned_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
with_activity AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM cleaned_readings s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.read_at < TIMESTAMP '2025-02-12 14:00:00'
              AND m.quality IN ('EST', 'OK')
        )
)
SELECT
    id,
    read_at,
    kwh,
    voltage
FROM with_activity
ORDER BY voltage, id
LIMIT 10
