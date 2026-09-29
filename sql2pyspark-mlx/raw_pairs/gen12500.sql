WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) li AS tier_value
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS FIRST) AS tier_value_percent_rank,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY tier_value NULLS FIRST) AS tier_value_cume_dist,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS LAST) AS tier_value_dense_rank,
        CUME_DIST() OVER (ORDER BY tier_value DESC) AS tier_value_cume_dist_2,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY tier_value DESC) AS tier_value_cume_dist_3
    FROM lines
    WHERE currency IN ('EUR', 'USD', 'THB')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        effective_from,
        currency,
        tier_value,
        tier_value_dense_rank,
        tier_value_cume_dist_3,
        MAX(tier_value_percent_rank) OVER w_ordered AS currency_max_tier_value_percent_rank,
        PERCENT_RANK() OVER w_recent AS tier_value_cume_dist_3_percent_rank,
        RANK() OVER w_recent AS tier_value_cume_dist_3_rank,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value_cume_dist_3 NULLS FIRST) AS tier_value_cume_dist_3_dense_rank
    FROM scored
    WINDOW w_ordered AS (PARTITION BY currency), w_recent AS (ORDER BY tier_value_cume_dist_3 NULLS FIRST)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        effective_from,
        currency,
        tier_value,
        tier_value_dense_rank,
        tier_value_cume_dist_3,
        currency_max_tier_value_percent_rank,
        tier_value_cume_dist_3_percent_rank,
        tier_value_cume_dist_3_rank,
        tier_value_cume_dist_3_dense_rank,
        ROUND(tier_value_dense_rank / tier_value_cume_dist_3_rank, 2) AS tier_value_dense_rank_per_tier_value_cume_dist_3_rank
    FROM metrics
    WHERE effective_from >= DATE '2024-01-26'
      AND tier_value_cume_dist_3_rank <= 7
),
totals AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('YEAR', effective_from) AS year_start,
        currency,
        ROUND(AVG(tier_value_cume_dist_3_rank), 2) AS avg_tier_value_cume_dist_3_rank,
        COUNT_IF(currency = 'USD') AS usd_events,
        SORT_ARRAY(COLLECT_SET(currency)) AS currency_list
    FROM base
    GROUP BY GROUPING SETS ((DATE_TRUNC('YEAR', effective_from), currency), (DATE_TRUNC('YEAR', effective_from)), ())
)
SELECT
    year_start,
    avg_tier_value_cume_dist_3_rank,
    usd_events,
    currency_list
FROM totals
ORDER BY year_start DESC NULLS FIRST
