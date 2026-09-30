WITH scoped AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage > 221.8
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at
    FROM scoped s
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT last_reading_read_at, quality, voltage
    FROM summarized_rows
) src
PIVOT (
    MIN(voltage)
    FOR quality IN ('OK', 'EST')
)
ORDER BY last_reading_read_at
