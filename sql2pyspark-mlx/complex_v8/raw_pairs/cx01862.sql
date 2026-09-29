WITH base AS (
    -- Scope the sites rows
    SELECT
        id,
        zone,
        tariff,
        installed_kw,
        commissioned_on
    FROM sites
    WHERE UPPER(TRIM(site_name)) LIKE 'M%'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        zone,
        installed_kw,
        commissioned_on,
        ROUND(AVG(installed_kw) OVER (ORDER BY installed_kw ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS installed_kw_avg_4,
        ROUND(installed_kw - AVG(installed_kw) OVER w_zone_tariff, 2) AS installed_kw_vs_zone_tariff_avg,
        ROUND(installed_kw - AVG(installed_kw) OVER (PARTITION BY MONTH(commissioned_on), tariff), 2) AS installed_kw_vs_commissioned_on_month_tariff_avg,
        DATEDIFF(commissioned_on, LAG(commissioned_on) OVER (ORDER BY installed_kw ASC, id)) AS days_since_prev_commissioned_on,
        SIZE(COLLECT_SET(zone) OVER (PARTITION BY MONTH(commissioned_on), tariff)) AS distinct_zone_in_commissioned_on_month_tariff,
        ROUND(CUME_DIST() OVER (PARTITION BY tariff, zone ORDER BY commissioned_on DESC), 3) AS commissioned_on_cume_dist,
        ROUND(CUME_DIST() OVER (PARTITION BY zone ORDER BY commissioned_on), 3) AS commissioned_on_cume_dist_2,
        LAG(commissioned_on) OVER (PARTITION BY tariff, zone ORDER BY commissioned_on DESC, id) AS prev_commissioned_on
    FROM base
    WINDOW w_zone_tariff AS (PARTITION BY zone, tariff)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(commissioned_on_cume_dist / NULLIF(SUM(commissioned_on_cume_dist) OVER (PARTITION BY distinct_zone_in_commissioned_on_month_tariff, MONTH(commissioned_on)), 0), 4) AS commissioned_on_cume_dist_share_of_distinct_zone,
        ROUND(PERCENT_RANK() OVER (ORDER BY commissioned_on_cume_dist_2 DESC), 3) AS commissioned_on_cume_dist_2_percent_rank,
        SUM(commissioned_on_cume_dist_2) OVER (PARTITION BY distinct_zone_in_commissioned_on_month_tariff, MONTH(commissioned_on)) AS distinct_zone_in_commissioned_on_month_tariff_co,
        LAST_VALUE(commissioned_on_cume_dist) IGNORE NULLS OVER (ORDER BY commissioned_on_cume_dist_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_commissioned_on_cume_dist
    FROM with_trends
)
SELECT
    id,
    zone,
    installed_kw,
    installed_kw_avg_4,
    installed_kw_vs_zone_tariff_avg,
    days_since_prev_commissioned_on,
    distinct_zone_in_commissioned_on_month_tariff,
    commissioned_on_cume_dist_2,
    prev_commissioned_on,
    commissioned_on_cume_dist_2_percent_rank,
    COALESCE(installed_kw, 0.0) AS installed_kw_filled
FROM with_windows
ORDER BY installed_kw
