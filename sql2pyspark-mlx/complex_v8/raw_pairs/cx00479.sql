WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage >= 216.1
),
with_parent_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        si.tariff AS site_tariff,
        si.installed_kw AS site_installed_kw,
        si.commissioned_on AS site_commissioned_on
    FROM recent_readings s
    JOIN sites si ON si.id = s.site_id
    WHERE si.commissioned_on >= DATE '2024-09-15'
),
joined AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        s.site_tariff,
        s.site_installed_kw,
        s.site_commissioned_on,
        si.commissioned_on AS site_commissioned_on_2
    FROM with_parent_site s
    JOIN sites si ON si.id = s.site_id
    WHERE si.commissioned_on BETWEEN DATE '2024-06-27' AND DATE '2025-11-28'
)
-- Subtotals with ROLLUP
SELECT
    site_id,
    COALESCE(site_tariff, 'ALL') AS site_tariff,
    SUM(CASE WHEN quality <> 'EST' THEN 1 ELSE 0 END) AS n_quality_est,
    MAX(read_at) AS last_read_at
FROM joined
GROUP BY ROLLUP(site_id, site_tariff)
ORDER BY site_id NULLS LAST, site_tariff NULLS LAST
