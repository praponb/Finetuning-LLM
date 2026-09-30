WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        tariff,
        commissioned_on,
        SUBSTRING(TRIM(site_name), 1, 4) AS site_name_prefix
    FROM sites
    WHERE YEAR(commissioned_on) = 2024
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        zone,
        tariff,
        site_name_prefix,
        LAG(commissioned_on, 2) OVER w_commissioned_on_year_zone_by AS prev2_commissioned_on,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', commissioned_on), tariff ORDER BY commissioned_on DESC, id) AS rn,
        LEAD(commissioned_on) OVER w_zone_by_commissioned_on AS next_commissioned_on,
        ROUND(CUME_DIST() OVER (PARTITION BY zone ORDER BY commissioned_on), 3) AS commissioned_on_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY tariff ORDER BY commissioned_on DESC, id), 3) AS commissioned_on_percent_rank,
        LEAD(commissioned_on) OVER (PARTITION BY DATE_TRUNC('MONTH', commissioned_on), tariff ORDER BY commissioned_on DESC, id) AS next_commissioned_on_2,
        NTILE(5) OVER (PARTITION BY tariff ORDER BY commissioned_on ASC, id) AS commissioned_on_quintile,
        DENSE_RANK() OVER (PARTITION BY tariff ORDER BY commissioned_on DESC, id) AS commissioned_on_dense_rank,
        FIRST_VALUE(commissioned_on) IGNORE NULLS OVER (PARTITION BY zone ORDER BY commissioned_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_commissioned_on,
        NTILE(3) OVER w_zone_by_commissioned_on AS commissioned_on_tercile,
        NTH_VALUE(commissioned_on, 2) OVER (PARTITION BY YEAR(commissioned_on), zone ORDER BY commissioned_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_commissioned_on,
        NTILE(10) OVER (PARTITION BY tariff ORDER BY commissioned_on DESC, id) AS commissioned_on_decile,
        LAST_VALUE(commissioned_on) OVER (PARTITION BY tariff ORDER BY commissioned_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_commissioned_on,
        DENSE_RANK() OVER w_commissioned_on_month_tariff AS commissioned_on_dense_rank_2,
        FIRST_VALUE(commissioned_on) OVER (PARTITION BY tariff ORDER BY commissioned_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_commissioned_on,
        NTILE(4) OVER (PARTITION BY DATE_TRUNC('MONTH', commissioned_on), tariff ORDER BY commissioned_on DESC, id) AS commissioned_on_quartile
    FROM prepared
    WINDOW w_commissioned_on_year_zone_by AS (PARTITION BY YEAR(commissioned_on), zone ORDER BY commissioned_on, id), w_zone_by_commissioned_on AS (PARTITION BY zone ORDER BY commissioned_on, id), w_commissioned_on_month_tariff AS (PARTITION BY DATE_TRUNC('MONTH', commissioned_on), tariff ORDER BY commissioned_on DESC, id)
)
SELECT
    id,
    next_commissioned_on_2,
    commissioned_on_quintile,
    commissioned_on_dense_rank,
    first_known_commissioned_on,
    commissioned_on_tercile,
    second_commissioned_on,
    commissioned_on_decile,
    commissioned_on_dense_rank_2,
    first_commissioned_on,
    commissioned_on_quartile,
    CONCAT_WS(' / ', zone, site_name_prefix) AS zone_site_name_prefix_label,
    ROUND(rn / NULLIF(commissioned_on_decile, 0), 3) AS rn_per_commissioned_on_decile
FROM windowed
ORDER BY id NULLS LAST
