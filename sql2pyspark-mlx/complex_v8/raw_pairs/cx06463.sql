WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage BETWEEN 219.6 AND 237.5
      AND site_id IS NOT NULL
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM recent_readings s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
)
SELECT *
FROM (
    SELECT read_at, quality, voltage
    FROM screened
) src
PIVOT (
    MIN(voltage)
    FOR quality IN ('OK', 'BAD')
)
ORDER BY read_at
