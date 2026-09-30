WITH filtered_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        INITCAP(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE kwh BETWEEN 14.4 AND 81.9
      AND read_at >= TIMESTAMP '2025-02-06 10:00:00'
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        voltage,
        quality,
        quality_clean,
        COALESCE(voltage, 0.0) AS voltage_filled
    FROM filtered_readings
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN m.quality NOT IN ('OK', 'BAD') THEN 1 END) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count
    FROM scored s
)
SELECT
    site_id,
    COALESCE(quality, 'ALL') AS quality,
    reading_quality_count,
    ROUND(AVG(voltage_filled), 2) AS avg_voltage_filled,
    MIN(reading_quality_count) AS min_reading_quality_count
FROM with_stats
GROUP BY CUBE(site_id, quality, reading_quality_count)
