WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        EXPLODE_OUTER(tier_prices) AS val
    FROM price_tiers
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        currency,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY val) val_cume_dist,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY val NULLS FIRST) val_cume_dist_2
    FROM flattened
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(val_cume_dist_2) OVER w_ordered currency_count_val_cume_dist_2,
        MAX(val_cume_dist_2) OVER w_ordered currency_max_val_cume_dist_2,
        AVG(val_cume_dist) OVER w_ordered currency_avg_val_cume_dist
    FROM with_windows
    WINDOW w_ordered AS (PARTITION BY currency)
),
enriched_price_tiers AS (
    -- Running and moving aggregates
    SELECT *, ROUND(currency_count_val_cume_dist_2 / SUM(currency_count_val_cume_dist_2) OVER (PARTITION BY currency), 4) currency_count_val_cume_dist_2_share
    FROM enriched
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        currency,
        currency_count_val_cume_dist_2,
        currency_max_val_cume_dist_2,
        currency_avg_val_cume_dist,
        currency_count_val_cume_dist_2_share / NULLIF(val_cume_dist, 0) currency_count_val_cume_dist_2_share_per_val_cume_dist
    FROM enriched_price_tiers
    WHERE currency_count_val_cume_dist_2_share >= 0.42
)
-- Grouped metrics
SELECT
    currency,
    SIZE(COLLECT_SET(currency)) AS num_currencies,
    ROUND(AVG(currency_max_val_cume_dist_2), 2) AS avg_currency_max_val_cume_dist_2,
    SIZE(COLLECT_SET(currency)) AS num_currencies_2,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list
FROM valid
GROUP BY currency
