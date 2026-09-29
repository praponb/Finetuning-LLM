WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage <= 240.2
      AND kwh BETWEEN 35.0 AND 116.8
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        DATE_TRUNC('YEAR', read_at) AS read_at_year,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM scoped
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(*) AS reading_count,
        ROUND(AVG(voltage), 2) AS avg_reading_voltage
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        m.avg_reading_voltage
    FROM annotated s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    read_at_year,
    quality_slug,
    reading_count,
    avg_reading_voltage,
    LOWER(REGEXP_REPLACE(TRIM(quality_slug), ' ', '_')) AS quality_slug_slug
FROM with_stats
ORDER BY voltage DESC, read_at_year DESC
