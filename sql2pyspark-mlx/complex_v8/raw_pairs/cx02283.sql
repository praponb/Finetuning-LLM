WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh <= 86.0
      AND read_at >= TIMESTAMP '2025-01-25 13:00:00'
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality,
        m.voltage AS reading_voltage
    FROM scoped s
    JOIN meter_readings m ON m.site_id = s.site_id
)
SELECT id, reading_id, measure, metric_value
FROM (
    SELECT id, reading_id, reading_kwh, voltage, kwh, reading_voltage
    FROM detail_rows_readings
) src
UNPIVOT (
    metric_value FOR measure IN (reading_kwh, voltage, kwh, reading_voltage)
) unp
