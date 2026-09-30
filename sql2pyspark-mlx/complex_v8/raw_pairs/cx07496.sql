WITH prepared AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        commissioned_on,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown
    FROM sites
    WHERE installed_kw < 373.3
),
grouped AS (
    -- Summarise per group
    SELECT
        tariff_or_unknown,
        COUNT(*) FILTER (WHERE zone <> 'Z4') AS n_zone_z4,
        ROUND(100 * COUNT(CASE WHEN UPPER(TRIM(site_name)) LIKE 'M%' THEN 1 END) / COUNT(*), 1) AS pct_upper_trim_site_name_like_m,
        SUM(CASE WHEN zone IN ('Z2', 'Z4') THEN 1 ELSE 0 END) AS n_zone_in_z2_z4,
        ARRAY_SORT(COLLECT_SET(site_name)) AS site_name_list,
        COUNT(DISTINCT tariff_or_unknown) AS distinct_tariff_or_unknowns
    FROM prepared
    WHERE commissioned_on >= DATE '2024-09-15'
    GROUP BY 1
),
metrics AS (
    -- Previous / next values per key
    SELECT
        tariff_or_unknown,
        n_zone_z4,
        pct_upper_trim_site_name_like_m,
        n_zone_in_z2_z4,
        distinct_tariff_or_unknowns,
        ROUND(AVG(n_zone_in_z2_z4) OVER (PARTITION BY n_zone_in_z2_z4 ORDER BY distinct_tariff_or_unknowns DESC, tariff_or_unknown ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS n_zone_in_z2_z4_avg_7,
        COUNT(*) OVER (PARTITION BY tariff_or_unknown) AS rows_in_tariff_or_unknown
    FROM grouped
),
metrics_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(rows_in_tariff_or_unknown) OVER (ORDER BY pct_upper_trim_site_name_like_m, tariff_or_unknown ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rows_in_tariff_or_unknown_sum_2,
        MIN(n_zone_in_z2_z4) OVER (ORDER BY pct_upper_trim_site_name_like_m, tariff_or_unknown ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_n_zone_in_z2_z4_so_far,
        LAG(n_zone_in_z2_z4_avg_7) OVER (ORDER BY pct_upper_trim_site_name_like_m, tariff_or_unknown) AS prev_n_zone_in_z2_z4_avg_7,
        n_zone_z4 - LAG(n_zone_z4) OVER (ORDER BY pct_upper_trim_site_name_like_m, tariff_or_unknown) AS n_zone_z4_change
    FROM metrics
)
SELECT
    tariff_or_unknown,
    n_zone_z4,
    pct_upper_trim_site_name_like_m,
    n_zone_in_z2_z4,
    distinct_tariff_or_unknowns,
    n_zone_in_z2_z4_avg_7,
    rows_in_tariff_or_unknown,
    min_n_zone_in_z2_z4_so_far,
    prev_n_zone_in_z2_z4_avg_7
FROM metrics_2
ORDER BY rows_in_tariff_or_unknown NULLS LAST, tariff_or_unknown NULLS LAST
