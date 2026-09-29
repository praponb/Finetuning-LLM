WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE (voltage <= 240.9 OR site_id IS NOT NULL)
),
with_details_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        si.site_name,
        si.tariff AS site_tariff,
        si.zone AS site_zone
    FROM base_readings s
    JOIN sites si ON si.id = s.site_id
),
combined_details_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        s.site_name,
        s.site_zone,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone_2,
        si.tariff AS site_tariff_2
    FROM with_details_site s
    JOIN sites si ON si.id = s.site_id
),
combined_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.site_name,
        s.site_zone,
        s.site_commissioned_on,
        s.site_zone_2,
        si.installed_kw AS site_installed_kw
    FROM combined_details_site s
    JOIN sites si ON si.id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    site_name,
    site_zone,
    site_commissioned_on,
    site_zone_2,
    site_installed_kw,
    DATE_FORMAT(site_commissioned_on, 'yyyy-MM-dd') AS site_commissioned_on_label,
    CASE WHEN site_installed_kw >= 373.3 THEN 'large' WHEN site_installed_kw >= 96.9 THEN 'medium' ELSE 'small' END AS site_installed_kw_tier
FROM combined_details
ORDER BY site_commissioned_on DESC
