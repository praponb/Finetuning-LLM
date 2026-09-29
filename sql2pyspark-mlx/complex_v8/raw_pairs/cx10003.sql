WITH matched AS (
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
with_details_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.prior_kwh_3d,
        si.commissioned_on AS site_commissioned_on,
        si.tariff AS site_tariff,
        si.site_name
    FROM matched s
    JOIN sites si ON si.id = s.site_id AND si.site_name IS NOT NULL
    WHERE LENGTH(TRIM(si.site_name)) >= 5
),
enriched_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.prior_kwh_3d,
        s.site_commissioned_on,
        s.site_name,
        si.installed_kw AS site_installed_kw,
        si.site_name AS site_site_name,
        si.tariff AS site_tariff_2
    FROM with_details_site s
    JOIN sites si ON si.id = s.site_id
),
without_matches AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM enriched_site s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, amount
FROM (
    SELECT id, prior_kwh_3d, site_installed_kw
    FROM without_matches
) src
UNPIVOT INCLUDE NULLS (
    amount FOR metric_name IN (prior_kwh_3d, site_installed_kw)
) unp
