WITH prepared_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
    WHERE quality = 'OK'
),
exploded_detail_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality_or_unknown,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.voltage AS reading_voltage
    FROM prepared_readings s
    JOIN meter_readings m ON m.site_id = s.site_id AND m.kwh > 50.4
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        quality_or_unknown,
        reading_id,
        reading_voltage,
        UPPER(TRIM(quality_or_unknown)) AS quality_or_unknown_clean,
        COALESCE(quality_or_unknown, 'UNKNOWN') AS quality_or_unknown_or_unknown
    FROM exploded_detail_readings
),
with_parent_reading AS (
    -- Attach reading attributes
    SELECT s.*, m.quality AS reading_quality
    FROM derived s
    JOIN meter_readings m ON m.id = s.reading_id AND HOUR(m.read_at) BETWEEN 8 AND 11
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    reading_id,
    reading_quality
FROM with_parent_reading
ORDER BY reading_id DESC
