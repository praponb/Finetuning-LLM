WITH filtered AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        tariff,
        installed_kw
    FROM sites
    WHERE installed_kw <= 284.0
      AND UPPER(TRIM(site_name)) LIKE 'S%'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        site_name,
        installed_kw,
        LEAD(installed_kw, 2) OVER (PARTITION BY tariff ORDER BY installed_kw DESC, id) AS next2_installed_kw,
        NTH_VALUE(installed_kw, 2) OVER (PARTITION BY tariff ORDER BY installed_kw DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_installed_kw,
        COUNT(*) OVER w_tariff AS rows_in_tariff,
        MAX(installed_kw) OVER (ORDER BY installed_kw DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS installed_kw_max_6_prior,
        LAG(installed_kw, 2) OVER (PARTITION BY tariff ORDER BY installed_kw DESC, id) AS prev2_installed_kw,
        ROUND(PERCENT_RANK() OVER w_tariff_by_installed_kw_ties, 3) AS installed_kw_percent_rank,
        FIRST_VALUE(installed_kw) IGNORE NULLS OVER (ORDER BY installed_kw DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_installed_kw,
        SUM(installed_kw) OVER (PARTITION BY tariff) AS tariff_sum_installed_kw,
        RANK() OVER (ORDER BY installed_kw DESC NULLS FIRST, id) AS installed_kw_rank,
        FIRST_VALUE(installed_kw) OVER (PARTITION BY tariff ORDER BY installed_kw DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_installed_kw,
        LAG(installed_kw, 2) OVER (ORDER BY installed_kw, id) AS prev2_installed_kw_2
    FROM filtered
    WINDOW w_tariff AS (PARTITION BY tariff), w_tariff_by_installed_kw_ties AS (PARTITION BY tariff ORDER BY installed_kw DESC)
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        site_name,
        next2_installed_kw,
        second_installed_kw,
        installed_kw_max_6_prior,
        prev2_installed_kw,
        installed_kw_percent_rank,
        first_known_installed_kw,
        tariff_sum_installed_kw,
        installed_kw_rank,
        first_installed_kw,
        prev2_installed_kw_2,
        ROUND(first_known_installed_kw / NULLIF(prev2_installed_kw_2, 0), 3) AS first_known_installed_kw_per_prev2_installed_kw_,
        ABS(rows_in_tariff - 10) AS rows_in_tariff_distance,
        SUBSTRING(TRIM(site_name), 1, 2) AS site_name_prefix
    FROM metrics
    WHERE prev2_installed_kw_2 >= 96.9
),
metrics_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_name,
        next2_installed_kw,
        second_installed_kw,
        installed_kw_max_6_prior,
        prev2_installed_kw,
        installed_kw_percent_rank,
        first_known_installed_kw,
        installed_kw_rank,
        first_installed_kw,
        prev2_installed_kw_2,
        first_known_installed_kw_per_prev2_installed_kw_,
        rows_in_tariff_distance,
        site_name_prefix,
        COUNT(*) OVER w_installed_kw_rank AS rows_in_installed_kw_rank,
        ROUND(installed_kw_rank / NULLIF(SUM(installed_kw_rank) OVER (PARTITION BY installed_kw_rank), 0), 4) AS installed_kw_rank_share_of_installed_kw_rank,
        MAX(next2_installed_kw) OVER (PARTITION BY installed_kw_rank ORDER BY first_known_installed_kw DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS next2_installed_kw_max_3,
        LAG(installed_kw_rank, 1, 0) OVER (PARTITION BY installed_kw_rank ORDER BY prev2_installed_kw_2 DESC, id) AS prev_installed_kw_rank,
        LAST_VALUE(first_known_installed_kw) OVER (PARTITION BY installed_kw_rank ORDER BY first_known_installed_kw DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_first_known_installed_kw
    FROM annotated
    WINDOW w_installed_kw_rank AS (PARTITION BY installed_kw_rank)
)
-- Final projection
SELECT
    id,
    second_installed_kw,
    installed_kw_max_6_prior,
    prev2_installed_kw,
    installed_kw_percent_rank,
    first_known_installed_kw,
    prev2_installed_kw_2,
    rows_in_tariff_distance,
    site_name_prefix,
    installed_kw_rank_share_of_installed_kw_rank,
    next2_installed_kw_max_3,
    prev_installed_kw_rank,
    last_first_known_installed_kw,
    UPPER(TRIM(site_name_prefix)) AS site_name_prefix_clean
FROM metrics_2
ORDER BY last_first_known_installed_kw NULLS LAST
