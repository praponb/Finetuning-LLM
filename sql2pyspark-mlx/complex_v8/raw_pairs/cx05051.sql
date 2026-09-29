WITH recent_sites AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on
    FROM sites
),
totals AS (
    -- Summarise per group
    SELECT
        tariff,
        zone,
        SUM(CASE WHEN zone = 'Z3' THEN installed_kw ELSE 0 END) AS installed_kw_zone_z3,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(CASE WHEN tariff = 'FLAT' THEN installed_kw ELSE 0 END) AS installed_kw_tariff_flat
    FROM recent_sites
    GROUP BY 1, 2
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN zone IN ('Z3', 'Z2') THEN 'group_a' WHEN zone = 'Z4' THEN 'group_b' ELSE 'other' END AS zone_group,
        ABS(installed_kw_tariff_flat - 1500.0) AS installed_kw_tariff_flat_distance,
        CONCAT(zone, '#', CAST(distinct_ids AS STRING)) AS zone_ref
    FROM totals
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(4) OVER w_all_by_installed_kw_tariff_f AS installed_kw_tariff_flat_distance_quartile,
        installed_kw_zone_z3 - LAG(installed_kw_zone_z3) OVER (PARTITION BY zone, zone_group ORDER BY installed_kw_tariff_flat_distance, tariff, zone) AS installed_kw_zone_z3_change,
        ROUND(CUME_DIST() OVER (ORDER BY installed_kw_tariff_flat_distance ASC), 3) AS installed_kw_tariff_flat_distance_cume_dist,
        COUNT(*) OVER (PARTITION BY distinct_ids) AS rows_in_distinct_ids,
        ROUND(AVG(installed_kw_tariff_flat) OVER w_distinct_ids, 2) AS distinct_ids_avg_installed_kw_tariff_flat
    FROM derived
    WINDOW w_all_by_installed_kw_tariff_f AS (ORDER BY installed_kw_tariff_flat_distance ASC, tariff, zone), w_distinct_ids AS (PARTITION BY distinct_ids)
),
zone_group_stats AS (
    -- Median rows_in_distinct_ids per zone_group
    SELECT zone_group, PERCENTILE_APPROX(rows_in_distinct_ids, 0.5) AS median_rows_in_distinct_ids
    FROM ranked
    GROUP BY zone_group
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_rows_in_distinct_ids,
        ROUND(s.rows_in_distinct_ids - g.median_rows_in_distinct_ids, 2) AS rows_in_distinct_ids_minus_median
    FROM ranked s
    JOIN zone_group_stats g ON g.zone_group = s.zone_group
    WHERE s.rows_in_distinct_ids >= g.median_rows_in_distinct_ids
),
latest AS (
    -- Keep the latest row per zone_group
    SELECT
        tariff,
        zone,
        installed_kw_zone_z3,
        distinct_ids,
        installed_kw_tariff_flat,
        zone_group,
        installed_kw_tariff_flat_distance,
        zone_ref,
        installed_kw_tariff_flat_distance_quartile,
        installed_kw_zone_z3_change,
        installed_kw_tariff_flat_distance_cume_dist,
        rows_in_distinct_ids,
        distinct_ids_avg_installed_kw_tariff_flat,
        median_rows_in_distinct_ids,
        rows_in_distinct_ids_minus_median
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY zone_group ORDER BY installed_kw_tariff_flat DESC, tariff DESC, zone DESC) AS recency_rank
        FROM vs_group
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    tariff,
    installed_kw_zone_z3,
    distinct_ids,
    installed_kw_tariff_flat,
    zone_group,
    installed_kw_tariff_flat_distance,
    zone_ref,
    installed_kw_tariff_flat_distance_quartile,
    installed_kw_tariff_flat_distance_cume_dist,
    median_rows_in_distinct_ids,
    ROUND(rows_in_distinct_ids / NULLIF(distinct_ids, 0), 3) AS rows_in_distinct_ids_per_distinct_ids
FROM latest
ORDER BY zone_ref DESC
