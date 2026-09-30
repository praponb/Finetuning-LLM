WITH valid_readings AS (
    -- Scope the meter_readings rows
    SELECT *
    FROM meter_readings
    WHERE voltage >= 223.3
      AND quality = 'OK'
),
totals AS (
    -- Group-level totals
    SELECT
        site_id,
        quality,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_ym,
        ROUND(AVG(kwh), 2) AS avg_kwh,
        MIN(kwh) AS min_kwh,
        ROUND(SUM(voltage), 2) AS total_voltage
    FROM valid_readings
    GROUP BY site_id, quality, DATE_FORMAT(read_at, 'yyyy-MM')
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT COUNT(*) FILTER (WHERE m.quality = 'EST') FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count
    FROM totals s
)
SELECT
    site_id,
    quality,
    read_at_ym,
    avg_kwh,
    min_kwh,
    reading_quality_count,
    GREATEST(reading_quality_count, reading_count) AS greatest_reading_quality_count_reading_count,
    LENGTH(TRIM(read_at_ym)) AS read_at_ym_len
FROM profiled
WHERE min_kwh > 12.0
ORDER BY min_kwh DESC, reading_quality_count DESC, site_id, quality, read_at_ym
LIMIT 5
