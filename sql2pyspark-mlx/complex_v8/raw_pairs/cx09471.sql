WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('HOUR', read_at) AS read_at_hour,
        LENGTH(TRIM(quality)) AS quality_len,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE quality = 'EST'
      AND site_id IS NOT NULL
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM scoped s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh <= 89.8
              AND TO_DATE(m.read_at) < DATE '2025-02-12'
        )
),
with_children_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality_ref,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality,
        m.voltage AS reading_voltage
    FROM qualified s
    JOIN meter_readings m ON m.site_id = s.site_id AND m.voltage < 239.6
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT site_id, reading_read_at, reading_quality, reading_voltage
    FROM with_children_readings
) src
PIVOT (
    SUM(reading_voltage)
    FOR reading_quality IN ('EST', 'BAD', 'OK')
)
