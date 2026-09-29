WITH prepared AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        commissioned_on,
        DATE_FORMAT(commissioned_on, 'yyyy-MM') AS commissioned_on_label,
        ABS(installed_kw - 141.6) AS installed_kw_distance
    FROM sites
    WHERE tariff = 'EV'
),
with_children_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_name,
        s.commissioned_on,
        s.commissioned_on_label,
        s.installed_kw_distance,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.read_at AS reading_read_at
    FROM prepared s
    JOIN meter_readings m ON m.site_id = s.id
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM with_children_readings s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.id
              AND MONTH(m.read_at) IN (2, 3)
              AND m.quality = 'EST'
        )
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        site_name,
        commissioned_on,
        reading_id,
        reading_quality,
        reading_read_at,
        CONCAT(reading_quality, '#', CAST(id AS STRING)) AS reading_quality_ref,
        DATE_FORMAT(reading_read_at, 'yyyy-MM-dd HH:00') AS reading_read_at_label
    FROM qualified
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MAX(voltage) AS max_reading_voltage, COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.max_reading_voltage,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM enriched s
    LEFT JOIN reading_stats m ON m.site_id = s.id
)
-- Final projection
SELECT
    id,
    site_name,
    commissioned_on,
    reading_id,
    reading_quality,
    reading_read_at,
    reading_quality_ref,
    reading_read_at_label,
    max_reading_voltage,
    reading_count,
    DATE_FORMAT(commissioned_on, 'MMM yyyy') AS commissioned_on_label,
    CONCAT_WS(': ', reading_quality_ref, reading_read_at_label) AS reading_quality_ref_reading_read_at_label_label
FROM with_stats
ORDER BY reading_id DESC
