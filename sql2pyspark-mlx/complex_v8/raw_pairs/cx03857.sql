WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        FLOOR(kwh / 25) * 25 AS kwh_bucket,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE kwh > 16.5
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT SUM(m.kwh) FROM meter_readings m WHERE m.site_id = s.site_id) AS total_reading_kwh,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count
    FROM prepared s
),
exploded_detail_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.total_reading_kwh,
        s.last_reading_read_at,
        s.reading_count,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.voltage AS reading_voltage,
        m.quality AS reading_quality,
        m.read_at AS reading_read_at
    FROM with_counts s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id AND m.voltage < 237.5
)
SELECT id, reading_id, metric, reading
FROM (
    SELECT id, reading_id, reading_voltage, total_reading_kwh, reading_kwh
    FROM exploded_detail_readings
) src
UNPIVOT (
    reading FOR metric IN (reading_voltage, total_reading_kwh, reading_kwh)
) unp
