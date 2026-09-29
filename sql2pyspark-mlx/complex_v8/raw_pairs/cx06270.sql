WITH filtered AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        DATE_FORMAT(commissioned_on, 'yyyy-MM-dd') AS commissioned_on_label
    FROM sites
    WHERE YEAR(commissioned_on) = 2025
      AND UPPER(TRIM(site_name)) LIKE 'S%'
),
totals AS (
    -- Group-level totals
    SELECT
        tariff,
        zone,
        COUNT(DISTINCT zone) AS distinct_zones,
        SUM(CASE WHEN zone = 'Z4' THEN 1 ELSE 0 END) AS n_zone_z4,
        ROUND(AVG(installed_kw), 2) AS avg_installed_kw,
        COUNT(*) AS cnt,
        COUNT(*) FILTER (WHERE UPPER(TRIM(site_name)) LIKE 'R%') AS n_upper_trim_site_name_like_r,
        SUM(installed_kw) FILTER (WHERE zone = 'Z3') AS installed_kw_zone_z3
    FROM filtered
    GROUP BY tariff, zone
    HAVING COUNT(*) >= 2
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM totals
    WHERE cnt < (SELECT MAX(cnt) FROM totals)
)
-- Native SQL UNPIVOT (wide to long)
SELECT tariff, zone, measure, metric_value
FROM (
    SELECT tariff, zone, avg_installed_kw, installed_kw_zone_z3
    FROM qualified
) src
UNPIVOT (
    metric_value FOR measure IN (avg_installed_kw, installed_kw_zone_z3)
) unp
ORDER BY tariff, zone, measure
