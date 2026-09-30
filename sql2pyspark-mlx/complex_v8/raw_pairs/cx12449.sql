WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE voltage <= 231.6
),
line_level_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.quality_slug,
        s.quality_ref,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage
    FROM filtered s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
),
detail_rows AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality_slug,
        s.reading_id,
        s.reading_quality,
        s.reading_voltage,
        m.id AS reading_id_2,
        m.quality AS reading_quality_2,
        m.read_at AS reading_read_at_2,
        m.voltage AS reading_voltage_2,
        m.kwh AS reading_kwh_2
    FROM line_level_readings s
    JOIN meter_readings m ON m.site_id = s.site_id
)
SELECT
    reading_id_2,
    site_id,
    COUNT(*) AS n_rows
FROM detail_rows
GROUP BY GROUPING SETS ((reading_id_2, site_id), (reading_id_2), ())
