WITH cleaned_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE quality = 'EST'
      AND read_at < TIMESTAMP '2025-02-12 12:00:00'
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM cleaned_readings s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
line_level AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.reading_count,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.read_at AS reading_read_at,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality
    FROM summarized_rows s
    JOIN meter_readings m ON m.site_id = s.site_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY reading_id ORDER BY reading_count NULLS LAST, id, reading_id) AS seq_no,
        SIZE(COLLECT_SET(reading_id) OVER (PARTITION BY reading_quality)) AS distinct_reading_id_in_reading_quality
    FROM line_level
),
leaders AS (
    -- First row per group
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        reading_count,
        reading_id,
        reading_voltage,
        reading_read_at,
        reading_kwh,
        reading_quality,
        distinct_reading_id_in_reading_quality
    FROM with_trends
    WHERE seq_no = 1
      AND HOUR(reading_read_at) BETWEEN 10 AND 12
)
SELECT
    id,
    site_id,
    kwh,
    voltage,
    reading_count,
    reading_id,
    reading_voltage,
    reading_kwh,
    reading_quality,
    LOWER(TRIM(reading_quality)) AS reading_quality_clean,
    ROUND((UNIX_TIMESTAMP(reading_read_at) - UNIX_TIMESTAMP(read_at)) / 3600, 1) AS hours_read_at_to_reading_read_at
FROM leaders
ORDER BY hours_read_at_to_reading_read_at DESC
