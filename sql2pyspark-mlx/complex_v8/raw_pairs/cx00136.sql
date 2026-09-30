WITH cleaned_sites AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        DATE_FORMAT(commissioned_on, 'yyyy-MM-dd') AS commissioned_on_label,
        ROUND(installed_kw * 0.25, 2) AS installed_kw_adj
    FROM sites
    WHERE commissioned_on < DATE '2024-11-21'
),
per_group AS (
    -- Group-level totals
    SELECT
        zone,
        tariff,
        ARRAY_SORT(COLLECT_SET(tariff)) AS tariff_list,
        MIN(installed_kw_adj) AS min_installed_kw_adj
    FROM cleaned_sites
    GROUP BY zone, tariff
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        zone,
        tariff,
        ROUND(PERCENT_RANK() OVER w_tariff_zone_by_min_installed, 3) AS min_installed_kw_adj_percent_rank,
        COUNT(*) OVER (ORDER BY min_installed_kw_adj, zone, tariff ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTH_VALUE(min_installed_kw_adj, 2) OVER (ORDER BY min_installed_kw_adj, zone, tariff ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_min_installed_kw_adj
    FROM per_group
    WINDOW w_tariff_zone_by_min_installed AS (PARTITION BY tariff, zone ORDER BY min_installed_kw_adj DESC NULLS FIRST, zone, tariff)
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_trends
    WHERE min_installed_kw_adj_percent_rank > (SELECT AVG(min_installed_kw_adj_percent_rank) FROM with_trends)
)
SELECT *
FROM (
    SELECT tariff, zone, min_installed_kw_adj_percent_rank
    FROM screened
) src
PIVOT (
    MAX(min_installed_kw_adj_percent_rank)
    FOR zone IN ('Z3', 'Z2', 'Z4')
)
ORDER BY tariff
