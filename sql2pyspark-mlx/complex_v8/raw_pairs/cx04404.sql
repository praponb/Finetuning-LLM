WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.kwh) AS prior_kwh_3d
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.site_id, a.read_at
),
enriched_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.prior_rows_3d,
        s.prior_kwh_3d,
        si.tariff AS site_tariff,
        si.installed_kw AS site_installed_kw,
        si.site_name
    FROM in_window s
    JOIN sites si ON si.id = s.site_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, value
FROM (
    SELECT id, prior_kwh_3d, site_installed_kw
    FROM enriched_site
) src
UNPIVOT (
    value FOR field IN (prior_kwh_3d, site_installed_kw)
) unp
