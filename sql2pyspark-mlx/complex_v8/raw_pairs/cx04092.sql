WITH prepared_sites AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        CASE WHEN installed_kw >= 386.6 THEN 'A' WHEN installed_kw >= 96.9 THEN 'B' ELSE 'C' END AS installed_kw_tier,
        DATE_TRUNC('MONTH', commissioned_on) AS commissioned_on_month
    FROM sites
    WHERE commissioned_on BETWEEN DATE '2024-06-27' AND DATE '2025-12-03'
      AND installed_kw <= 284.0
),
with_activity AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM prepared_sites s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.id
              AND m.kwh BETWEEN 29.2 AND 117.2
              AND m.quality IS NOT NULL
        )
      AND s.zone IS NOT NULL
)
SELECT
    zone,
    tariff,
    GROUPING_ID() AS grouping_level,
    ROUND(AVG(installed_kw), 2) AS avg_installed_kw
FROM with_activity
GROUP BY GROUPING SETS ((zone, tariff), (zone), ())
ORDER BY zone NULLS LAST, tariff NULLS LAST
