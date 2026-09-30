WITH filtered_sites AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        CASE WHEN installed_kw >= 386.6 THEN 'A' WHEN installed_kw >= 141.6 THEN 'B' ELSE 'C' END AS installed_kw_tier,
        DATE_TRUNC('YEAR', commissioned_on) AS commissioned_on_year
    FROM sites
    WHERE tariff IS NOT NULL
      AND site_name IS NOT NULL
),
qualified AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM filtered_sites s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.id
              AND m.quality NOT IN ('EST', 'BAD')
              AND HOUR(m.read_at) BETWEEN 8 AND 12
        )
      AND MONTH(s.commissioned_on_year) IN (1, 3)
)
-- Final projection
SELECT
    id,
    zone,
    tariff,
    installed_kw,
    installed_kw_tier,
    DATEDIFF(DATE '2026-01-01', commissioned_on_year) AS days_since_commissioned_on_year
FROM qualified
ORDER BY zone, installed_kw_tier DESC
