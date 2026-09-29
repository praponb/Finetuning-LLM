WITH base AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        LENGTH(TRIM(site_name)) AS site_name_len
    FROM sites
    WHERE commissioned_on >= DATE '2024-09-15'
),
per_group AS (
    -- Group-level totals
    SELECT
        zone,
        site_name_len,
        COUNT(DISTINCT zone) AS distinct_zones,
        COUNT(*) FILTER (WHERE LENGTH(TRIM(site_name)) >= 4) AS n_length_trim_site_name_4,
        ARRAY_SORT(COLLECT_SET(site_name)) AS site_name_list,
        COUNT(*) AS cnt,
        SUM(CASE WHEN zone IN ('Z4', 'Z2') THEN installed_kw ELSE 0 END) AS installed_kw_zone_in_z4_z2
    FROM base
    GROUP BY 1, 2
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(installed_kw_zone_in_z4_z2, 3) OVER (PARTITION BY cnt, n_length_trim_site_name_4 ORDER BY distinct_zones ASC, zone, site_name_len) AS prev3_installed_kw_zone_in_z4_z2,
        NTH_VALUE(installed_kw_zone_in_z4_z2, 2) OVER (PARTITION BY cnt, n_length_trim_site_name_4 ORDER BY distinct_zones ASC, zone, site_name_len ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_installed_kw_zone_in_z4_z2,
        ROW_NUMBER() OVER (PARTITION BY cnt, n_length_trim_site_name_4 ORDER BY distinct_zones ASC, zone, site_name_len) AS distinct_zones_seq
    FROM per_group
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(zone)) AS zone_len,
        CASE WHEN distinct_zones >= 6 THEN 'HIGH' WHEN distinct_zones >= 5 THEN 'MEDIUM' ELSE 'LOW' END AS distinct_zones_tier,
        distinct_zones - cnt AS distinct_zones_minus_cnt
    FROM with_windows
    WHERE installed_kw_zone_in_z4_z2 >= 250.0
),
distinct_zones_tier_stats AS (
    -- Avg second_installed_kw_zone_in_z4_z2 per distinct_zones_tier
    SELECT distinct_zones_tier, AVG(second_installed_kw_zone_in_z4_z2) AS avg_second_installed_kw_zone_in_z4_z2
    FROM labeled
    GROUP BY distinct_zones_tier
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_second_installed_kw_zone_in_z4_z2,
        ROUND(s.second_installed_kw_zone_in_z4_z2 - g.avg_second_installed_kw_zone_in_z4_z2, 2) AS second_installed_kw_zone_in_z4_z2_minus_avg
    FROM labeled s
    JOIN distinct_zones_tier_stats g ON g.distinct_zones_tier = s.distinct_zones_tier
    WHERE s.second_installed_kw_zone_in_z4_z2 > g.avg_second_installed_kw_zone_in_z4_z2
)
SELECT
    zone,
    site_name_len,
    n_length_trim_site_name_4,
    site_name_list,
    cnt,
    installed_kw_zone_in_z4_z2,
    distinct_zones_seq,
    zone_len,
    distinct_zones_tier,
    distinct_zones_minus_cnt,
    avg_second_installed_kw_zone_in_z4_z2,
    second_installed_kw_zone_in_z4_z2_minus_avg
FROM compared
ORDER BY distinct_zones_tier, n_length_trim_site_name_4 NULLS LAST
