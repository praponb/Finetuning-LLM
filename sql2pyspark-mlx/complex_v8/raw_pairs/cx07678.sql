WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.voltage) AS prior_voltage_3d
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.site_id, a.read_at
),
combined_details_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.prior_voltage_3d,
        si.zone AS site_zone,
        si.site_name,
        si.installed_kw AS site_installed_kw
    FROM covered s
    JOIN sites si ON si.id = s.site_id
    WHERE LENGTH(TRIM(si.site_name)) >= 5
)
-- Final projection
SELECT
    id,
    read_at,
    prior_voltage_3d,
    site_name,
    site_installed_kw,
    COALESCE(site_zone, 'UNKNOWN') AS site_zone_or_unknown,
    CASE WHEN site_installed_kw >= 386.6 THEN 'HIGH' WHEN site_installed_kw >= 141.6 THEN 'MEDIUM' ELSE 'LOW' END AS site_installed_kw_tier
FROM combined_details_site
WHERE site_id IS NOT NULL
ORDER BY site_installed_kw_tier DESC, site_installed_kw DESC NULLS FIRST
