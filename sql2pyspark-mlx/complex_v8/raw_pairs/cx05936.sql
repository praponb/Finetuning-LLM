WITH base AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LOWER(TRIM(quality)) AS quality_clean,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        TO_DATE(read_at) AS read_at_date
    FROM meter_readings
    WHERE kwh < 110.5
      AND site_id IS NOT NULL
),
combined_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        s.quality_clean,
        s.quality_or_unknown,
        s.read_at_date,
        si.commissioned_on AS site_commissioned_on
    FROM base s
    JOIN sites si ON si.id = s.site_id
),
filtered AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM combined_details s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
        )
      AND s.read_at_date BETWEEN DATE '2024-05-01' AND DATE '2025-06-01'
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at
    FROM filtered s
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    quality,
    quality_clean,
    quality_or_unknown,
    read_at_date,
    site_commissioned_on,
    last_reading_read_at,
    ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(last_reading_read_at)) / 60, 1) AS minutes_last_reading_read_at_to_read_at,
    YEAR(read_at_date) AS read_at_date_year
FROM profiled
ORDER BY site_commissioned_on, quality
