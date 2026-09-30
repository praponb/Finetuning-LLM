WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE quality = 'BAD'
      AND kwh <= 109.3
),
exploded_detail AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh,
        m.voltage AS reading_voltage
    FROM cleaned_readings s
    JOIN meter_readings m ON m.site_id = s.site_id AND m.kwh BETWEEN 30.2 AND 81.1
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(reading_read_at)) / 3600, 1) AS hours_reading_read_at_to_read_at,
        COALESCE(kwh, 0.0) AS kwh_filled,
        CONCAT(reading_quality, '#', CAST(reading_id AS STRING)) AS reading_quality_ref
    FROM exploded_detail
)
-- Final projection
SELECT
    id,
    site_id,
    quality,
    reading_id,
    reading_read_at,
    reading_quality,
    reading_kwh,
    reading_voltage,
    hours_reading_read_at_to_read_at,
    reading_quality_ref,
    ROUND(reading_kwh / NULLIF(kwh, 0), 3) AS reading_kwh_per_kwh
FROM derived
ORDER BY reading_quality_ref DESC NULLS FIRST
