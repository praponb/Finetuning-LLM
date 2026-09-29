WITH prepared_sites AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        ABS(installed_kw - 283.9) AS installed_kw_distance
    FROM sites
    WHERE tariff IS NOT NULL
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_name,
        zone,
        tariff,
        commissioned_on,
        installed_kw_distance,
        LEAD(commissioned_on) OVER w_all_by_installed_kw AS next_commissioned_on,
        DENSE_RANK() OVER (PARTITION BY zone ORDER BY installed_kw_distance DESC NULLS FIRST, id) AS installed_kw_distance_dense_rank,
        LAST_VALUE(installed_kw) OVER (ORDER BY installed_kw DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_installed_kw,
        COUNT(*) OVER (PARTITION BY tariff, zone) AS rows_in_tariff_zone,
        ROUND(PERCENT_RANK() OVER (PARTITION BY tariff ORDER BY installed_kw_distance ASC, id), 3) AS installed_kw_distance_percent_rank,
        LEAD(installed_kw_distance, 1, 0) OVER (ORDER BY installed_kw DESC, id) AS next_installed_kw_distance,
        MAX(installed_kw_distance) OVER (PARTITION BY tariff, zone) AS tariff_zone_max_installed_kw_distance,
        SUM(installed_kw_distance) OVER w_tariff_zone AS tariff_zone_sum_installed_kw_distance,
        RANK() OVER (ORDER BY installed_kw DESC) AS installed_kw_rank,
        DENSE_RANK() OVER (ORDER BY installed_kw DESC) AS installed_kw_dense_rank,
        ROUND(installed_kw / NULLIF(SUM(installed_kw) OVER (PARTITION BY tariff, zone), 0), 4) AS installed_kw_share_of_tariff_zone,
        ROUND(AVG(installed_kw) OVER w_tariff_zone, 2) AS tariff_zone_avg_installed_kw,
        MIN(installed_kw_distance) OVER (PARTITION BY tariff, zone) AS tariff_zone_min_installed_kw_distance
    FROM prepared_sites
    WINDOW w_all_by_installed_kw AS (ORDER BY installed_kw DESC, id), w_tariff_zone AS (PARTITION BY tariff, zone)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(installed_kw_rank) OVER (PARTITION BY tariff ORDER BY last_installed_kw ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS installed_kw_rank_avg_2_prior,
        SUM(installed_kw_dense_rank) OVER (PARTITION BY tariff ORDER BY last_installed_kw ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_installed_kw_dense_rank,
        SUM(rows_in_tariff_zone) OVER (PARTITION BY zone, tariff) AS zone_tariff_sum_rows_in_tariff_zone,
        SUM(installed_kw_dense_rank) OVER (PARTITION BY zone, MONTH(next_commissioned_on) ORDER BY tariff_zone_max_installed_kw_distance DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS installed_kw_dense_rank_sum_3_prior,
        ROUND(installed_kw_distance - AVG(installed_kw_distance) OVER w_installed_kw_distance_dense, 2) AS installed_kw_distance_vs_installed_kw_distance_d,
        SUM(installed_kw_dense_rank) OVER (PARTITION BY zone, tariff) AS zone_tariff_sum_installed_kw_dense_rank,
        ROUND(installed_kw_rank / NULLIF(SUM(installed_kw_rank) OVER (PARTITION BY installed_kw_distance_dense_rank), 0), 4) AS installed_kw_rank_share_of_installed_kw_distance,
        LEAD(installed_kw_dense_rank) OVER (ORDER BY installed_kw_distance_percent_rank ASC, id) AS next_installed_kw_dense_rank
    FROM ranked
    WINDOW w_installed_kw_distance_dense AS (PARTITION BY installed_kw_distance_dense_rank)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        tariff,
        COUNT(*) AS n_rows,
        SUM(installed_kw_dense_rank) FILTER (WHERE zone = 'Z4') AS installed_kw_dense_rank_zone_z4,
        SUM(CASE WHEN site_name IS NOT NULL THEN running_installed_kw_dense_rank ELSE 0 END) AS running_installed_kw_dense_rank_site_name_is_not,
        DATEDIFF(MAX(commissioned_on), MIN(commissioned_on)) AS commissioned_on_span_days,
        SUM(rows_in_tariff_zone) AS total_rows_in_tariff_zone
    FROM with_windows
    WHERE site_name IS NOT NULL
    GROUP BY tariff
)
-- Final projection
SELECT
    tariff,
    installed_kw_dense_rank_zone_z4,
    running_installed_kw_dense_rank_site_name_is_not,
    commissioned_on_span_days,
    LOWER(REGEXP_REPLACE(TRIM(tariff), ' ', '_')) AS tariff_slug,
    COALESCE(n_rows, 0) AS n_rows_filled
FROM rollup_base
ORDER BY commissioned_on_span_days, tariff
LIMIT 20
