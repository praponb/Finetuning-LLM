WITH filtered_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM meter_readings
    WHERE quality = 'OK'
),
enriched_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        si.commissioned_on AS site_commissioned_on,
        si.tariff AS site_tariff,
        si.zone AS site_zone
    FROM filtered_readings s
    LEFT JOIN sites si ON si.id = s.site_id AND si.zone = 'Z2'
),
exploded_detail AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        s.site_tariff,
        s.site_zone,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality
    FROM enriched_site s
    JOIN meter_readings m ON m.site_id = s.site_id
),
line_level_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.site_tariff,
        s.site_zone,
        s.reading_id,
        s.reading_quality,
        m.id AS reading_id_2,
        m.read_at AS reading_read_at_2,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality_2
    FROM exploded_detail s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    site_tariff,
    site_zone,
    reading_id,
    reading_quality,
    reading_id_2,
    reading_read_at_2,
    reading_kwh,
    reading_quality_2,
    DATE_FORMAT(reading_read_at_2, 'yyyy-MM-dd HH:00') AS reading_read_at_2_label,
    LOWER(REGEXP_REPLACE(TRIM(reading_quality), ' ', '_')) AS reading_quality_slug
FROM line_level_readings
ORDER BY reading_kwh DESC, id, reading_id, reading_id_2
LIMIT 10
