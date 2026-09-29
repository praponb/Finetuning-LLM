WITH cleaned_sites AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        ABS(installed_kw - 283.9) AS installed_kw_distance,
        LENGTH(TRIM(zone)) AS zone_len
    FROM sites
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(tariff) OVER (PARTITION BY zone, zone_len)) AS distinct_tariff_in_zone_zone_len,
        zone_len - LAG(zone_len) OVER (PARTITION BY zone_len, zone ORDER BY installed_kw_distance DESC, id) AS zone_len_change,
        LAG(zone_len) OVER (PARTITION BY zone_len, zone ORDER BY installed_kw_distance DESC, id) AS prev_zone_len,
        NTH_VALUE(installed_kw, 2) OVER (PARTITION BY zone_len, zone ORDER BY installed_kw_distance DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_installed_kw
    FROM cleaned_sites
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.id) AS reading_count
    FROM with_trends s
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN zone IN ('Z4', 'Z2') THEN 'group_a' WHEN zone = 'Z3' THEN 'group_b' ELSE 'other' END AS zone_group,
        ABS(zone_len_change - -4) AS zone_len_change_distance,
        ROUND(installed_kw / NULLIF(zone_len, 0), 3) AS installed_kw_per_zone_len
    FROM profiled
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(installed_kw - AVG(installed_kw) OVER (PARTITION BY reading_count), 2) AS installed_kw_vs_reading_count_avg,
        ROUND(zone_len_change - AVG(zone_len_change) OVER (PARTITION BY reading_count), 2) AS zone_len_change_vs_reading_count_avg
    FROM scored
)
-- Final projection
SELECT
    id,
    site_name,
    zone,
    tariff,
    zone_len,
    distinct_tariff_in_zone_zone_len,
    prev_zone_len,
    second_installed_kw,
    zone_len_change_distance,
    installed_kw_per_zone_len,
    installed_kw_vs_reading_count_avg
FROM ranked
ORDER BY site_name, installed_kw_per_zone_len DESC
