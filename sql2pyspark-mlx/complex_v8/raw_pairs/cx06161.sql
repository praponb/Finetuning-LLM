WITH recent AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh < 104.8
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(CASE WHEN quality = 'OK' THEN 1 END) AS reading_quality_count
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count
    FROM recent s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
with_parent_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.reading_quality_count,
        si.zone AS site_zone
    FROM summarized_rows s
    JOIN sites si ON si.id = s.site_id
)
-- Final projection
SELECT
    id,
    kwh,
    site_zone,
    LOWER(REGEXP_REPLACE(TRIM(site_zone), ' ', '_')) AS site_zone_slug,
    CASE WHEN site_zone IN ('Z2', 'Z3') THEN 'group_a' WHEN site_zone = 'Z4' THEN 'group_b' ELSE 'other' END AS site_zone_group
FROM with_parent_site
ORDER BY site_zone_slug DESC NULLS FIRST, site_zone DESC NULLS FIRST
