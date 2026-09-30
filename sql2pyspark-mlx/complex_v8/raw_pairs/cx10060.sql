WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        ROUND(kwh * 1.07, 2) AS kwh_adj
    FROM meter_readings
    WHERE kwh BETWEEN 34.6 AND 106.5
),
with_parent_site AS (
    -- Join sites
    SELECT
        s.*,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on,
        si.tariff AS site_tariff
    FROM valid s
    JOIN sites si ON si.id = s.site_id
    WHERE si.tariff = 'EV'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        kwh_adj,
        site_zone,
        site_commissioned_on,
        site_tariff,
        NTILE(10) OVER w_read_at_month_by_read_at AS read_at_decile,
        MAX(kwh) OVER (PARTITION BY YEAR(read_at)) AS read_at_year_max_kwh,
        ROUND(kwh_adj - AVG(kwh_adj) OVER (PARTITION BY site_id), 2) AS kwh_adj_vs_site_id_avg,
        SIZE(COLLECT_SET(site_tariff) OVER w_site_id) AS distinct_site_tariff_in_site_id,
        MAX(kwh_adj) OVER (PARTITION BY site_id) AS site_id_max_kwh_adj,
        NTH_VALUE(kwh_adj, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at) ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh_adj,
        LAG(kwh_adj, 3) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at) ORDER BY read_at DESC, id) AS prev3_kwh_adj,
        ROUND(AVG(kwh_adj) OVER (PARTITION BY YEAR(read_at)), 2) AS read_at_year_avg_kwh_adj,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', read_at) ORDER BY read_at DESC) AS read_at_dense_rank,
        LEAD(kwh_adj) OVER w_quality_site_commissioned_on AS next_kwh_adj,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on), quality ORDER BY read_at ASC, id), 3) AS read_at_percent_rank,
        SIZE(COLLECT_SET(site_zone) OVER (PARTITION BY site_id)) AS distinct_site_zone_in_site_id,
        LEAD(read_at) OVER (PARTITION BY site_tariff ORDER BY read_at ASC, id) AS next_read_at,
        MAX(kwh_adj) OVER (PARTITION BY site_tariff ORDER BY read_at ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS kwh_adj_max_3,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY YEAR(read_at)), 0), 4) AS kwh_share_of_read_at_year
    FROM with_parent_site
    WINDOW w_read_at_month_by_read_at AS (PARTITION BY DATE_TRUNC('MONTH', read_at) ORDER BY read_at DESC, id), w_site_id AS (PARTITION BY site_id), w_quality_site_commissioned_on AS (PARTITION BY quality, YEAR(site_commissioned_on) ORDER BY read_at DESC, id)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(kwh_share_of_read_at_year) OVER (PARTITION BY distinct_site_zone_in_site_id, site_zone ORDER BY read_at_decile, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh_share_of_read_at_year,
        NTH_VALUE(kwh_share_of_read_at_year, 2) OVER (PARTITION BY distinct_site_zone_in_site_id, site_zone ORDER BY read_at_decile, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh_share_of_read_at_year,
        LAST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY read_at_dense_rank, quality ORDER BY kwh_share_of_read_at_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_kwh,
        ROUND(kwh_adj_vs_site_id_avg - AVG(kwh_adj_vs_site_id_avg) OVER (PARTITION BY read_at_dense_rank, read_at_decile), 2) AS kwh_adj_vs_site_id_avg_vs_read_at_dense_rank_rea,
        NTH_VALUE(next_kwh_adj, 2) OVER (PARTITION BY read_at_dense_rank, quality ORDER BY kwh_share_of_read_at_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_next_kwh_adj,
        ROUND(AVG(kwh_share_of_read_at_year) OVER (PARTITION BY read_at_dense_rank, quality ORDER BY kwh_share_of_read_at_year ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS kwh_share_of_read_at_year_avg_7
    FROM windowed
)
SELECT
    id,
    kwh,
    quality,
    kwh_adj,
    site_zone,
    site_tariff,
    read_at_decile,
    read_at_year_max_kwh,
    kwh_adj_vs_site_id_avg,
    distinct_site_tariff_in_site_id,
    second_kwh_adj,
    prev3_kwh_adj,
    read_at_year_avg_kwh_adj,
    next_kwh_adj,
    distinct_site_zone_in_site_id,
    next_read_at,
    kwh_adj_max_3,
    kwh_share_of_read_at_year,
    running_kwh_share_of_read_at_year,
    kwh_adj_vs_site_id_avg_vs_read_at_dense_rank_rea,
    second_next_kwh_adj,
    kwh_share_of_read_at_year_avg_7,
    DATE_TRUNC('QUARTER', next_read_at) AS next_read_at_quarter,
    ROUND(second_next_kwh_adj / NULLIF(read_at_percent_rank, 0), 3) AS second_next_kwh_adj_per_read_at_percent_rank
FROM ranked
ORDER BY kwh_adj_vs_site_id_avg NULLS LAST
