WITH filtered_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        HOUR(read_at) AS read_at_hour,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage
    FROM meter_readings
),
annotated AS (
    -- Derive helper columns
    SELECT *, DATEDIFF(DATE '2026-01-01', read_at) AS days_since_read_at
    FROM filtered_readings
    WHERE site_id >= 3
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(*) AS reading_count,
        MAX(read_at) AS last_reading_read_at,
        MAX(kwh) AS max_reading_kwh
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        m.last_reading_read_at,
        m.max_reading_kwh
    FROM annotated s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    voltage,
    quality,
    read_at_hour,
    kwh_per_voltage,
    days_since_read_at,
    reading_count,
    last_reading_read_at
FROM summarized_rows
ORDER BY last_reading_read_at DESC, quality NULLS LAST
