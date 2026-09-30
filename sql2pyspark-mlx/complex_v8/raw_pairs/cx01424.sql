WITH valid AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh >= 14.5
),
with_parent_site AS (
    -- Join sites
    SELECT s.*, si.tariff AS site_tariff, si.site_name
    FROM valid s
    JOIN sites si ON si.id = s.site_id
),
joined_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.site_tariff,
        s.site_name,
        si.installed_kw AS site_installed_kw,
        si.tariff AS site_tariff_2
    FROM with_parent_site s
    JOIN sites si ON si.id = s.site_id
    WHERE si.installed_kw < 373.3
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT SUM(m.kwh) FROM meter_readings m WHERE m.site_id = s.site_id) AS total_reading_kwh,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count
    FROM joined_site s
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        GREATEST(reading_count, site_installed_kw) AS greatest_reading_count_site_installed_kw,
        CASE WHEN site_tariff IN ('FLAT', 'EV') THEN 'group_a' WHEN site_tariff = 'TOU' THEN 'group_b' ELSE 'other' END AS site_tariff_group
    FROM profiled
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    voltage,
    quality,
    site_tariff,
    site_name,
    site_installed_kw,
    site_tariff_2,
    total_reading_kwh,
    greatest_reading_count_site_installed_kw,
    site_tariff_group,
    COALESCE(kwh, 0.0) AS kwh_filled
FROM enriched
WHERE reading_count >= 2
ORDER BY voltage DESC NULLS FIRST, site_id
