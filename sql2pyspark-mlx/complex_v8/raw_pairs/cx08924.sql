WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 10 AND 15
),
enriched AS (
    -- Derive helper columns
    SELECT *, SUBSTRING(TRIM(quality), 1, 2) AS quality_prefix
    FROM scoped
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MIN(read_at) AS first_reading_read_at,
        MAX(read_at) AS last_reading_read_at,
        COUNT(*) FILTER (WHERE quality IN ('EST', 'BAD')) AS reading_quality_count
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.first_reading_read_at,
        m.last_reading_read_at,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count
    FROM enriched s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_TRUNC('HOUR', read_at) AS read_at_hour,
        ABS(voltage - 227.3) AS voltage_distance,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM with_stats
    WHERE first_reading_read_at >= TIMESTAMP '2025-01-09 09:00:00'
)
SELECT
    reading_quality_count,
    quality_or_unknown,
    quality,
    MIN(voltage_distance) AS min_voltage_distance,
    ROUND(AVG(reading_quality_count), 2) AS avg_reading_quality_count,
    ROUND(AVG(voltage), 2) AS avg_voltage
FROM derived
GROUP BY GROUPING SETS ((reading_quality_count, quality_or_unknown, quality), (reading_quality_count), ())
ORDER BY reading_quality_count NULLS LAST, quality_or_unknown NULLS LAST, quality NULLS LAST
