WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE site_id >= 7
),
line_level AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.quality_slug,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality
    FROM recent s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
),
combined_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.reading_id,
        s.reading_kwh,
        s.reading_read_at,
        si.tariff AS site_tariff,
        si.zone AS site_zone
    FROM line_level s
    JOIN sites si ON si.id = s.site_id
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        reading_id,
        reading_kwh,
        CASE WHEN site_tariff IN ('TOU', 'EV') THEN 'group_a' WHEN site_tariff = 'FLAT' THEN 'group_b' ELSE 'other' END AS site_tariff_group,
        FLOOR(reading_kwh / 25) * 25 AS reading_kwh_bucket
    FROM combined_details
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count_2
    FROM tagged s
)
SELECT id, reading_id, measure, value
FROM (
    SELECT id, reading_id, reading_count, reading_kwh_bucket
    FROM with_stats
) src
UNPIVOT INCLUDE NULLS (
    value FOR measure IN (reading_count AS m_reading_count, reading_kwh_bucket AS m_reading_kwh_bucket)
) unp
ORDER BY id, reading_id, measure
