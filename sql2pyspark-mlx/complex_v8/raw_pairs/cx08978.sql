WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.voltage) AS prior_voltage_12h
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.site_id, a.read_at
),
combined_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.prior_rows_12h,
        si.zone AS site_zone,
        si.installed_kw AS site_installed_kw
    FROM joined_ranges s
    JOIN sites si ON si.id = s.site_id
)
SELECT
    id,
    prior_rows_12h,
    site_zone,
    site_installed_kw,
    LENGTH(TRIM(site_zone)) AS site_zone_len
FROM combined_details_site
ORDER BY site_zone_len DESC, site_installed_kw, id
LIMIT 5
