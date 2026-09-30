WITH scoped AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        tariff,
        installed_kw
    FROM sites
    WHERE commissioned_on BETWEEN DATE '2024-08-24' AND DATE '2025-05-10'
      AND tariff <> 'FLAT'
),
without_matches AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM scoped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.id
              AND m.quality = 'OK'
              AND m.kwh BETWEEN 10.9 AND 113.0
        )
),
tariff_stats AS (
    -- Median installed_kw per tariff
    SELECT tariff, PERCENTILE_APPROX(installed_kw, 0.5) AS median_installed_kw
    FROM without_matches
    GROUP BY tariff
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_installed_kw,
        ROUND(s.installed_kw - g.median_installed_kw, 2) AS installed_kw_minus_median
    FROM without_matches s
    JOIN tariff_stats g ON g.tariff = s.tariff
    WHERE s.installed_kw >= g.median_installed_kw
),
qualified AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM vs_group s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.id
              AND m.read_at >= TIMESTAMP '2025-01-19 09:00:00'
        )
)
-- Final projection
SELECT
    id,
    site_name,
    tariff,
    installed_kw,
    ROUND(installed_kw / NULLIF(median_installed_kw, 0), 3) AS installed_kw_per_median_installed_kw
FROM qualified
WHERE installed_kw > 96.9
ORDER BY id, installed_kw NULLS LAST
