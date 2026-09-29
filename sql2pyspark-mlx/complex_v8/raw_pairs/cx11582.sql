WITH scoped AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE (voltage BETWEEN 216.8 AND 235.8 OR kwh >= 43.1)
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at
    FROM scoped s
),
with_children_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.last_reading_read_at,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.kwh AS reading_kwh
    FROM with_stats s
    JOIN meter_readings m ON m.site_id = s.site_id
)
SELECT id, reading_id, measure, reading
FROM (
    SELECT id, reading_id, voltage, reading_kwh
    FROM with_children_readings
) src
UNPIVOT (
    reading FOR measure IN (voltage AS voltage_v, reading_kwh AS m_reading_kwh)
) unp
ORDER BY id, reading_id, measure
