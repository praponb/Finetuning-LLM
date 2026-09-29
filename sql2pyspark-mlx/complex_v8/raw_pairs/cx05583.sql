WITH prepared AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref,
        DATE_TRUNC('QUARTER', read_at) AS read_at_quarter,
        LOWER(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE MONTH(read_at) IN (2, 3)
),
combined_details_site AS (
    -- Join sites
    SELECT
        s.*,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw
    FROM prepared s
    JOIN sites si ON si.id = s.site_id
),
with_details_site AS (
    -- Join sites
    SELECT s.*, si.installed_kw AS site_installed_kw_2
    FROM combined_details_site s
    JOIN sites si ON si.id = s.site_id
)
-- Subtotals with ROLLUP
SELECT
    site_id,
    quality,
    GROUPING(site_id) AS is_total_site_id,
    ROUND(AVG(site_installed_kw), 2) AS avg_site_installed_kw,
    ROUND(SUM(site_installed_kw_2), 2) AS total_site_installed_kw_2
FROM with_details_site
GROUP BY ROLLUP(site_id, quality)
ORDER BY site_id NULLS LAST, quality NULLS LAST
