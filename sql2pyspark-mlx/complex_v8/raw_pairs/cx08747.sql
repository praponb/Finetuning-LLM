WITH cleaned_sites AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on
    FROM sites
    WHERE commissioned_on BETWEEN DATE '2024-09-15' AND DATE '2025-05-10'
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN m.quality = 'EST' THEN 1 END) FROM meter_readings m WHERE m.site_id = s.id) AS reading_quality_count,
        (SELECT COUNT(CASE WHEN m.quality IN ('OK', 'BAD') THEN 1 END) FROM meter_readings m WHERE m.site_id = s.id) AS reading_quality_count_2,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.id) AS last_reading_read_at
    FROM cleaned_sites s
),
with_activity AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM profiled s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.id
              AND TO_DATE(m.read_at) < DATE '2025-02-12'
        )
),
reading_stats_2 AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MAX(read_at) AS last_reading_read_at_2,
        MAX(read_at) AS last_reading_read_at_3,
        COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
profiled_2 AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.last_reading_read_at_2,
        m.last_reading_read_at_3,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM with_activity s
    LEFT JOIN reading_stats_2 m ON m.site_id = s.id
),
with_children_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.zone,
        s.installed_kw,
        s.reading_quality_count,
        s.reading_quality_count_2,
        s.last_reading_read_at,
        s.last_reading_read_at_3,
        s.reading_count,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality
    FROM profiled_2 s
    LEFT JOIN meter_readings m ON m.site_id = s.id
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM with_children_readings s
    WHERE s.id IN (
            SELECT m.site_id
            FROM meter_readings m
            WHERE m.kwh >= 45.3
        )
),
line_level AS (
    -- One row per reading
    SELECT
        s.id,
        s.zone,
        s.installed_kw,
        s.reading_quality_count,
        s.reading_quality_count_2,
        s.last_reading_read_at,
        s.last_reading_read_at_3,
        s.reading_count,
        s.reading_id,
        s.reading_voltage,
        m.id AS reading_id_2,
        m.read_at AS reading_read_at_2,
        m.quality AS reading_quality_2,
        m.voltage AS reading_voltage_2
    FROM qualified s
    JOIN meter_readings m ON m.site_id = s.id
)
-- Final projection
SELECT
    id,
    installed_kw,
    reading_quality_count,
    reading_quality_count_2,
    last_reading_read_at,
    last_reading_read_at_3,
    reading_count,
    reading_id,
    reading_voltage,
    reading_id_2,
    reading_read_at_2,
    reading_quality_2,
    reading_voltage_2
FROM line_level
ORDER BY reading_voltage_2
