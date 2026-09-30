WITH base AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        ABS(installed_kw - 373.3) AS installed_kw_distance,
        FLOOR(installed_kw / 100) * 100 AS installed_kw_bucket
    FROM sites
    WHERE LENGTH(TRIM(site_name)) >= 4
),
per_group AS (
    -- Group-level totals
    SELECT
        zone,
        tariff,
        ARRAY_SORT(COLLECT_SET(site_name)) AS site_name_list,
        SUM(installed_kw_bucket) FILTER (WHERE tariff IN ('EV', 'TOU')) AS installed_kw_bucket_tariff_in_ev_tou,
        ROUND(SUM(installed_kw_distance) / NULLIF(SUM(installed_kw_bucket), 0), 3) AS installed_kw_distance_to_installed_kw_bucket_rat,
        ROUND(STDDEV(installed_kw), 2) AS stddev_installed_kw
    FROM base
    GROUP BY zone, tariff
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(stddev_installed_kw) OVER (PARTITION BY zone ORDER BY installed_kw_bucket_tariff_in_ev_tou DESC, zone, tariff ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS stddev_installed_kw_avg_4,
        ROUND(stddev_installed_kw - AVG(stddev_installed_kw) OVER (PARTITION BY zone), 2) AS stddev_installed_kw_vs_zone_avg,
        RANK() OVER (PARTITION BY zone ORDER BY installed_kw_bucket_tariff_in_ev_tou DESC) AS installed_kw_bucket_tariff_in_ev_tou_rank,
        ROW_NUMBER() OVER (PARTITION BY zone ORDER BY installed_kw_bucket_tariff_in_ev_tou DESC, zone, tariff) AS rn,
        MIN(stddev_installed_kw) OVER (PARTITION BY zone) AS zone_min_stddev_installed_kw,
        MAX(stddev_installed_kw) OVER (PARTITION BY zone) AS zone_max_stddev_installed_kw
    FROM per_group
)
-- Native SQL UNPIVOT (wide to long)
SELECT zone, tariff, measure, value
FROM (
    SELECT zone, tariff, stddev_installed_kw_avg_4, installed_kw_distance_to_installed_kw_bucket_rat, zone_min_stddev_installed_kw
    FROM with_windows
) src
UNPIVOT (
    value FOR measure IN (stddev_installed_kw_avg_4, installed_kw_distance_to_installed_kw_bucket_rat, zone_min_stddev_installed_kw)
) unp
