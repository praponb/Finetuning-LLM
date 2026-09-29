WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh < 101.2
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        ROUND(AVG(voltage), 2) AS avg_reading_voltage,
        MAX(read_at) AS last_reading_read_at,
        COUNT(*) FILTER (WHERE quality <> 'BAD') AS reading_quality_count
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.avg_reading_voltage,
        m.last_reading_read_at,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count
    FROM base s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
SELECT
    site_id,
    COALESCE(quality, 'ALL') AS quality,
    reading_quality_count,
    ROUND(SUM(voltage), 2) AS total_voltage,
    SUM(reading_quality_count) AS total_reading_quality_count
FROM profiled
GROUP BY GROUPING SETS ((site_id, quality, reading_quality_count), (site_id), (quality), ())
