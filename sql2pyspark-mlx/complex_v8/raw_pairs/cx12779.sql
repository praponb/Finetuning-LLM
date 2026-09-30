WITH prepared_sites AS (
    -- Filter sites
    SELECT
        id,
        zone,
        tariff,
        installed_kw,
        DATE_FORMAT(commissioned_on, 'yyyy-MM') AS commissioned_on_label
    FROM sites
    WHERE zone = 'Z4'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        zone,
        tariff,
        ROUND(installed_kw - AVG(installed_kw) OVER (PARTITION BY tariff, zone), 2) AS installed_kw_vs_tariff_zone_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY zone ORDER BY installed_kw), 3) AS installed_kw_percent_rank,
        DENSE_RANK() OVER (PARTITION BY zone ORDER BY installed_kw) AS installed_kw_dense_rank,
        ROUND(installed_kw / NULLIF(SUM(installed_kw) OVER w_tariff_zone, 0), 4) AS installed_kw_share_of_tariff_zone,
        NTH_VALUE(installed_kw, 2) OVER (PARTITION BY zone ORDER BY installed_kw, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_installed_kw
    FROM prepared_sites
    WINDOW w_tariff_zone AS (PARTITION BY tariff, zone)
),
leaders AS (
    -- Top 2 per group
    SELECT
        id,
        zone,
        tariff,
        installed_kw_vs_tariff_zone_avg,
        installed_kw_percent_rank,
        installed_kw_share_of_tariff_zone,
        second_installed_kw
    FROM windowed
    WHERE installed_kw_dense_rank <= 2
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM leaders
    WHERE installed_kw_percent_rank > (SELECT AVG(installed_kw_percent_rank) FROM leaders)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(second_installed_kw, 2) OVER (ORDER BY installed_kw_vs_tariff_zone_avg DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_second_installed_kw,
        DENSE_RANK() OVER w_all_by_installed_kw_vs_tarif_2 AS installed_kw_vs_tariff_zone_avg_dense_rank
    FROM screened
    WINDOW w_all_by_installed_kw_vs_tarif_2 AS (ORDER BY installed_kw_vs_tariff_zone_avg DESC NULLS FIRST)
)
-- Final projection
SELECT
    id,
    zone,
    tariff,
    installed_kw_vs_tariff_zone_avg,
    installed_kw_percent_rank,
    installed_kw_share_of_tariff_zone,
    second_installed_kw,
    second_second_installed_kw,
    installed_kw_vs_tariff_zone_avg_dense_rank
FROM ranked
WHERE second_second_installed_kw BETWEEN 66.5 AND 386.6
ORDER BY installed_kw_vs_tariff_zone_avg, installed_kw_vs_tariff_zone_avg_dense_rank DESC
