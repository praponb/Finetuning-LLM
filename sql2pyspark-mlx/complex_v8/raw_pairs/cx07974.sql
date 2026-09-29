WITH scoped_sites AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        COALESCE(installed_kw, 0.0) AS installed_kw_filled
    FROM sites
    WHERE commissioned_on BETWEEN DATE '2024-09-15' AND DATE '2025-05-07'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(installed_kw) OVER (PARTITION BY tariff, zone) AS tariff_zone_max_installed_kw,
        ROW_NUMBER() OVER (PARTITION BY tariff ORDER BY installed_kw_filled DESC, id) AS installed_kw_filled_seq,
        SUM(installed_kw) OVER (PARTITION BY tariff ORDER BY installed_kw_filled DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS installed_kw_sum_4,
        COUNT(*) OVER (PARTITION BY tariff ORDER BY installed_kw_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(AVG(installed_kw) OVER (PARTITION BY tariff, zone), 2) AS tariff_zone_avg_installed_kw
    FROM scoped_sites
),
zone_stats AS (
    -- Avg installed_kw_sum_4 per zone
    SELECT zone, AVG(installed_kw_sum_4) AS avg_installed_kw_sum_4
    FROM with_trends
    GROUP BY zone
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_installed_kw_sum_4,
        ROUND(s.installed_kw_sum_4 - g.avg_installed_kw_sum_4, 2) AS installed_kw_sum_4_minus_avg
    FROM with_trends s
    JOIN zone_stats g ON g.zone = s.zone
    WHERE s.installed_kw_sum_4 > g.avg_installed_kw_sum_4
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER w_all_by_installed_kw_filled_t AS installed_kw_filled_dense_rank,
        ROW_NUMBER() OVER w_all_by_tariff_zone_max_insta AS tariff_zone_max_installed_kw_seq,
        SIZE(COLLECT_SET(zone) OVER (PARTITION BY tariff)) AS distinct_zone_in_tariff,
        COUNT(*) OVER w_tariff AS rows_in_tariff
    FROM outliers
    WINDOW w_all_by_installed_kw_filled_t AS (ORDER BY installed_kw_filled ASC), w_all_by_tariff_zone_max_insta AS (ORDER BY tariff_zone_max_installed_kw, id), w_tariff AS (PARTITION BY tariff)
),
windowed_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        installed_kw_filled,
        tariff_zone_max_installed_kw,
        installed_kw_sum_4,
        running_count,
        tariff_zone_avg_installed_kw,
        avg_installed_kw_sum_4,
        installed_kw_sum_4_minus_avg,
        installed_kw_filled_dense_rank,
        tariff_zone_max_installed_kw_seq,
        distinct_zone_in_tariff,
        rows_in_tariff,
        ROUND(running_count / NULLIF(SUM(running_count) OVER w_installed_kw_filled_dense_ra, 0), 4) AS running_count_share_of_installed_kw_filled_dense,
        ROUND(installed_kw_sum_4 - AVG(installed_kw_sum_4) OVER w_installed_kw_filled_dense_ra, 2) AS installed_kw_sum_4_vs_installed_kw_filled_dense_,
        ROUND(CUME_DIST() OVER (ORDER BY installed_kw DESC), 3) AS installed_kw_cume_dist,
        NTILE(10) OVER (ORDER BY rows_in_tariff, id) AS rows_in_tariff_decile
    FROM windowed
    WINDOW w_installed_kw_filled_dense_ra AS (PARTITION BY installed_kw_filled_dense_rank)
)
SELECT
    id,
    site_name,
    zone,
    tariff,
    installed_kw_filled,
    tariff_zone_max_installed_kw,
    installed_kw_sum_4,
    running_count,
    tariff_zone_avg_installed_kw,
    avg_installed_kw_sum_4,
    installed_kw_sum_4_minus_avg,
    tariff_zone_max_installed_kw_seq,
    rows_in_tariff,
    running_count_share_of_installed_kw_filled_dense,
    installed_kw_cume_dist,
    rows_in_tariff_decile,
    COALESCE(installed_kw_filled, 0.0) AS installed_kw_filled_filled
FROM windowed_2
ORDER BY tariff_zone_avg_installed_kw DESC
