WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh > 48.2
      AND site_id <= 6
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at
    FROM base_readings s
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        quality,
        last_reading_read_at,
        DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label,
        read_at + INTERVAL 2 HOURS AS read_at_plus_2h
    FROM summarized_rows
    WHERE last_reading_read_at >= TIMESTAMP '2025-01-17 09:00:00'
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at
    FROM enriched s
)
-- Final projection
SELECT
    id,
    read_at,
    quality,
    read_at_label,
    first_reading_read_at,
    SUBSTRING(TRIM(quality), 1, 2) AS quality_prefix,
    INITCAP(TRIM(quality)) AS quality_clean
FROM profiled
ORDER BY quality_prefix DESC
