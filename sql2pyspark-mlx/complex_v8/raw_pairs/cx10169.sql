WITH valid_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        GREATEST(kwh, voltage) AS greatest_kwh_voltage,
        ABS(kwh - 42.1) AS kwh_distance
    FROM meter_readings
    WHERE voltage > 216.5
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        si.installed_kw AS site_installed_kw,
        si.commissioned_on AS site_commissioned_on,
        si.tariff AS site_tariff
    FROM valid_readings s
    JOIN sites si ON si.id = s.site_id
),
with_details AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        s.kwh_distance,
        s.site_tariff,
        si.site_name
    FROM with_parent s
    JOIN sites si ON si.id = s.site_id
),
without_matches AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM with_details s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh < 87.1
        )
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM without_matches s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality IS NOT NULL
        )
),
with_details_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        s.kwh_distance,
        s.site_tariff,
        s.site_name,
        si.installed_kw AS site_installed_kw
    FROM screened s
    JOIN sites si ON si.id = s.site_id
    WHERE si.installed_kw >= 96.9
)
-- Subtotals with CUBE
SELECT
    COALESCE(site_tariff, 'ALL') AS site_tariff,
    site_id,
    COALESCE(quality, 'ALL') AS quality,
    COUNT(*) FILTER (WHERE site_name IS NOT NULL) AS n_site_name_is_not_null,
    COUNT(*) FILTER (WHERE site_tariff NOT IN ('TOU', 'FLAT')) AS n_site_tariff_not_in_tou_flat,
    ARRAY_SORT(COLLECT_SET(quality)) AS quality_list
FROM with_details_site
GROUP BY CUBE(site_tariff, site_id, quality)
