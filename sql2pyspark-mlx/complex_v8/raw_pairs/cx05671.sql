WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
    WHERE site_id >= 4
),
line_level AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.voltage AS reading_voltage,
        m.read_at AS reading_read_at
    FROM cleaned s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id AND m.voltage BETWEEN 212.7 AND 233.1
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN m.quality IS NOT NULL THEN 1 END) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count
    FROM line_level s
)
SELECT
    quality,
    reading_count,
    GROUPING(quality) AS is_total_quality,
    MAX(reading_read_at) AS last_reading_read_at,
    COUNT(*) FILTER (WHERE quality IN ('OK', 'BAD')) AS n_quality_in_ok_bad
FROM summarized_rows
GROUP BY ROLLUP(quality, reading_count)
