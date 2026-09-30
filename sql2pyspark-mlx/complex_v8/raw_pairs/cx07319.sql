WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
    WHERE kwh >= 20.3
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at,
        (SELECT MIN(m.voltage) FROM meter_readings m WHERE m.site_id = s.site_id) AS min_reading_voltage
    FROM scoped s
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        quality,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref
    FROM profiled
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count_2
    FROM tagged s
)
SELECT
    COALESCE(quality, 'ALL') AS quality,
    COALESCE(quality_or_unknown, 'ALL') AS quality_or_unknown,
    reading_count,
    GROUPING(quality) AS is_total_quality,
    ROUND(STDDEV(reading_count_2), 2) AS stddev_reading_count_2,
    ROUND(AVG(reading_count_2), 2) AS avg_reading_count_2
FROM summarized_rows
GROUP BY GROUPING SETS ((quality, quality_or_unknown, reading_count), (quality), ())
ORDER BY quality NULLS LAST, quality_or_unknown NULLS LAST, reading_count NULLS LAST
