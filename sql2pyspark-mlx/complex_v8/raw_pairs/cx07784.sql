WITH scoped_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        voltage - kwh AS voltage_minus_kwh
    FROM meter_readings
    WHERE TO_DATE(read_at) < DATE '2025-03-08'
      AND voltage >= 223.1
),
combined_details_site AS (
    -- Join sites
    SELECT s.*, si.installed_kw AS site_installed_kw
    FROM scoped_readings s
    JOIN sites si ON si.id = s.site_id
)
-- Subtotals with CUBE
SELECT
    site_id,
    quality,
    COUNT(*) AS cnt
FROM combined_details_site
GROUP BY CUBE(site_id, quality)
ORDER BY site_id NULLS LAST, quality NULLS LAST
