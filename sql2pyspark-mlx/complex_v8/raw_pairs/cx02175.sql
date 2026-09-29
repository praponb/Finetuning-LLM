WITH valid AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        commissioned_on,
        ROUND(installed_kw * 1.07, 2) AS installed_kw_adj,
        CASE WHEN zone IN ('Z2', 'Z3') THEN 'group_a' WHEN zone = 'Z4' THEN 'group_b' ELSE 'other' END AS zone_group,
        CASE WHEN zone IN ('Z4', 'Z2') THEN 'group_a' WHEN zone = 'Z3' THEN 'group_b' ELSE 'other' END AS zone_group_2
    FROM sites
    WHERE zone = 'Z4'
      AND LENGTH(TRIM(site_name)) >= 6
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        site_name,
        zone,
        tariff,
        zone_group,
        COALESCE(installed_kw_adj, 0.0) AS installed_kw_adj_filled
    FROM valid
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(installed_kw_adj_filled, 2) OVER (PARTITION BY zone, tariff ORDER BY installed_kw_adj_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_installed_kw_adj_filled,
        ROUND(AVG(installed_kw_adj_filled) OVER (PARTITION BY zone_group, tariff), 2) AS zone_group_tariff_avg_installed_kw_adj_filled,
        ROW_NUMBER() OVER w_all_by_installed_kw_adj_fill AS rn,
        MAX(installed_kw_adj_filled) OVER w_zone_group_tariff AS zone_group_tariff_max_installed_kw_adj_filled,
        NTILE(4) OVER (PARTITION BY zone, zone_group ORDER BY installed_kw_adj_filled, id) AS installed_kw_adj_filled_quartile
    FROM scored
    WINDOW w_all_by_installed_kw_adj_fill AS (ORDER BY installed_kw_adj_filled DESC, id), w_zone_group_tariff AS (PARTITION BY zone_group, tariff)
)
SELECT
    id,
    site_name,
    zone,
    zone_group,
    installed_kw_adj_filled,
    second_installed_kw_adj_filled,
    zone_group_tariff_avg_installed_kw_adj_filled,
    zone_group_tariff_max_installed_kw_adj_filled
FROM ranked
ORDER BY zone_group_tariff_avg_installed_kw_adj_filled, zone_group_tariff_max_installed_kw_adj_filled
