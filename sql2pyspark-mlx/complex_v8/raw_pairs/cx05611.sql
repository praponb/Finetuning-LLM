WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        FLOOR(kwh / 25) * 25 AS kwh_bucket
    FROM meter_readings
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at
    FROM base_readings s
),
line_level AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.kwh_bucket,
        s.first_reading_read_at,
        s.last_reading_read_at,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality
    FROM summarized_rows s
    JOIN meter_readings m ON m.site_id = s.site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT ROUND(AVG(m.kwh), 2) FROM meter_readings m WHERE m.site_id = s.site_id) AS avg_reading_kwh
    FROM line_level s
)
-- Final projection
SELECT
    id,
    read_at,
    kwh,
    kwh_bucket,
    last_reading_read_at,
    reading_voltage,
    reading_read_at,
    reading_count,
    avg_reading_kwh,
    reading_count + reading_voltage AS reading_count_plus_reading_voltage,
    COALESCE(kwh_bucket, 0) AS kwh_bucket_filled
FROM with_counts
WHERE reading_read_at < TIMESTAMP '2025-03-08 09:00:00'
ORDER BY read_at DESC, id
