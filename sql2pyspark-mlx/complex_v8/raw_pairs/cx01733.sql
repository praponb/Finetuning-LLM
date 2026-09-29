WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        tariff,
        installed_kw,
        ABS(installed_kw - 283.9) AS installed_kw_distance,
        LOWER(REGEXP_REPLACE(TRIM(tariff), ' ', '_')) AS tariff_slug
    FROM sites
    WHERE commissioned_on BETWEEN DATE '2024-06-27' AND DATE '2025-11-28'
      AND tariff = 'EV'
),
summary AS (
    -- Summarise per group
    SELECT
        tariff,
        ROUND(SUM(installed_kw_distance) / NULLIF(SUM(installed_kw), 0), 3) AS installed_kw_distance_to_installed_kw_ratio,
        ROUND(STDDEV(installed_kw_distance), 2) AS stddev_installed_kw_distance,
        ARRAY_SORT(COLLECT_SET(tariff)) AS tariff_list,
        ROUND(SUM(installed_kw), 2) AS total_installed_kw
    FROM valid
    GROUP BY 1
    HAVING AVG(installed_kw) > 96.9
)
SELECT tariff, measure, metric_value
FROM (
    SELECT tariff, installed_kw_distance_to_installed_kw_ratio, stddev_installed_kw_distance, total_installed_kw
    FROM summary
) src
UNPIVOT (
    metric_value FOR measure IN (installed_kw_distance_to_installed_kw_ratio, stddev_installed_kw_distance, total_installed_kw)
) unp
