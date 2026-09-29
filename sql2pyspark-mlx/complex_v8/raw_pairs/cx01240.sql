WITH base_sites AS (
    -- Scope the sites rows
    SELECT
        id,
        tariff,
        installed_kw,
        commissioned_on,
        YEAR(commissioned_on) AS commissioned_on_year,
        CONCAT(zone, '#', CAST(id AS STRING)) AS zone_ref,
        CONCAT_WS(' / ', tariff, zone) AS tariff_zone_label
    FROM sites
    WHERE zone IN ('Z4', 'Z3')
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        tariff,
        commissioned_on,
        tariff_zone_label,
        NTH_VALUE(installed_kw, 2) OVER (ORDER BY commissioned_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_installed_kw,
        ROUND(commissioned_on_year - AVG(commissioned_on_year) OVER w_commissioned_on_year, 2) AS commissioned_on_year_vs_commissioned_on_year_avg,
        MAX(installed_kw) OVER (PARTITION BY commissioned_on_year) AS commissioned_on_year_max_installed_kw
    FROM base_sites
    WINDOW w_commissioned_on_year AS (PARTITION BY commissioned_on_year)
)
SELECT id, metric, reading
FROM (
    SELECT id, second_installed_kw, commissioned_on_year_max_installed_kw
    FROM with_trends
) src
UNPIVOT (
    reading FOR metric IN (second_installed_kw, commissioned_on_year_max_installed_kw)
) unp
