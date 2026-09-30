WITH base AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE quality IS NOT NULL
),
line_level AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.quality AS reading_quality,
        m.read_at AS reading_read_at,
        m.kwh AS reading_kwh
    FROM base s
    JOIN meter_readings m ON m.site_id = s.site_id
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MIN(kwh) AS min_reading_kwh, COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.min_reading_kwh,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM line_level s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        s.reading_id,
        s.reading_quality,
        s.reading_read_at,
        s.reading_kwh,
        s.reading_count,
        si.site_name
    FROM with_stats s
    JOIN sites si ON si.id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    voltage,
    reading_id,
    reading_quality,
    reading_read_at,
    reading_kwh,
    reading_count,
    site_name,
    CASE WHEN reading_count >= 5 THEN 'large' WHEN reading_count >= 2 THEN 'medium' ELSE 'small' END AS reading_count_tier,
    DATEDIFF(DATE '2026-01-01', reading_read_at) AS days_since_reading_read_at
FROM with_details
WHERE voltage >= 216.8
ORDER BY id
