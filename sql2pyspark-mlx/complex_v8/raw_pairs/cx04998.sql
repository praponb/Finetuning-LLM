WITH base AS (
    -- Scope the sites rows
    SELECT *
    FROM sites
    WHERE commissioned_on BETWEEN DATE '2024-06-27' AND DATE '2025-05-10'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        MAX(installed_kw) OVER (PARTITION BY tariff, YEAR(commissioned_on) ORDER BY installed_kw, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS installed_kw_max_6_prior,
        LAG(installed_kw) OVER (PARTITION BY zone ORDER BY installed_kw, id) AS prev_installed_kw,
        ROUND(installed_kw - AVG(installed_kw) OVER (PARTITION BY zone, tariff), 2) AS installed_kw_vs_zone_tariff_avg,
        ROUND(AVG(installed_kw) OVER (PARTITION BY zone ORDER BY installed_kw DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS installed_kw_avg_7,
        installed_kw - LAG(installed_kw) OVER w_zone_by_installed_kw_2 AS installed_kw_change,
        ROUND(CUME_DIST() OVER (PARTITION BY zone ORDER BY installed_kw), 3) AS installed_kw_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY zone ORDER BY installed_kw, id) AS seq_no,
        LAST_VALUE(commissioned_on) OVER (PARTITION BY zone ORDER BY installed_kw, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_commissioned_on,
        COUNT(*) OVER w_zone_tariff AS rows_in_zone_tariff,
        SUM(installed_kw) OVER (PARTITION BY zone ORDER BY installed_kw, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS installed_kw_sum_3_prior,
        RANK() OVER w_tariff_commissioned_on_year_2 AS installed_kw_rank,
        MIN(installed_kw) OVER (PARTITION BY zone ORDER BY installed_kw DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_installed_kw_so_far,
        RANK() OVER w_zone_by_installed_kw_ties AS installed_kw_rank_2
    FROM base
    WINDOW w_zone_by_installed_kw_2 AS (PARTITION BY zone ORDER BY installed_kw DESC, id), w_zone_tariff AS (PARTITION BY zone, tariff), w_tariff_commissioned_on_year_2 AS (PARTITION BY tariff, YEAR(commissioned_on) ORDER BY installed_kw), w_zone_by_installed_kw_ties AS (PARTITION BY zone ORDER BY installed_kw)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(installed_kw_rank_2) OVER (PARTITION BY zone), 2) AS zone_avg_installed_kw_rank_2,
        FIRST_VALUE(installed_kw) OVER (PARTITION BY seq_no, installed_kw_rank ORDER BY installed_kw_vs_zone_tariff_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_installed_kw,
        RANK() OVER (PARTITION BY tariff ORDER BY rows_in_zone_tariff DESC, id) AS rows_in_zone_tariff_rank,
        ROUND(PERCENT_RANK() OVER w_tariff_by_installed_kw_chang, 3) AS installed_kw_change_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY seq_no, installed_kw_rank ORDER BY installed_kw_vs_zone_tariff_avg DESC, id), 3) AS installed_kw_vs_zone_tariff_avg_cume_dist
    FROM metrics
    WINDOW w_tariff_by_installed_kw_chang AS (PARTITION BY tariff ORDER BY installed_kw_change NULLS LAST)
)
SELECT
    id,
    site_name,
    zone,
    installed_kw,
    prev_installed_kw,
    installed_kw_vs_zone_tariff_avg,
    installed_kw_avg_7,
    installed_kw_change,
    installed_kw_cume_dist,
    last_commissioned_on,
    installed_kw_sum_3_prior,
    installed_kw_rank,
    min_installed_kw_so_far,
    installed_kw_rank_2,
    first_installed_kw,
    rows_in_zone_tariff_rank,
    installed_kw_change_percent_rank,
    installed_kw_vs_zone_tariff_avg_cume_dist
FROM with_trends
WHERE rows_in_zone_tariff_rank < 9
ORDER BY installed_kw_change_percent_rank DESC, id
LIMIT 10
