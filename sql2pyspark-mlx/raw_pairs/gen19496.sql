WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) e AS tier_value
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS LAST) AS tier_value_percent_rank,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY tier_value DESC NULLS FIRST) AS tier_value_dense_rank,
        COUNT(tier_value) OVER (PARTITION BY currency) AS currency_count_tier_value
    FROM flattened
    WHERE plan <> 'pro'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, tier_value_percent_rank + currency_count_tier_value AS tier_value_percent_rank_plus_currency_count_tier_value
    FROM with_windows
    WHERE plan IN ('starter', 'pro')
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, currency
    FROM filtered
    EXCEPT DISTINCT
    SELECT id, currency
    FROM price_tiers
    WHERE currency <> 'USD'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.id = k.id AND s.currency = k.currency
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        plan,
        currency,
        tier_value,
        tier_value_percent_rank,
        tier_value_percent_rank_plus_currency_count_tier_value,
        CONCAT_WS('-', plan, currency) AS plan_currency_key
    FROM kept_rows
    WHERE tier_value BETWEEN 22 AND 26
),
with_windows_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        currency,
        plan_currency_key,
        CUME_DIST() OVER (ORDER BY tier_value ASC NULLS LAST) AS tier_value_cume_dist,
        MAX(tier_value_percent_rank_plus_currency_count_tier_value) OVER (PARTITION BY plan) AS plan_max_tier_value_percent_rank_plus_currency_count_tier_value,
        STDDEV_SAMP(tier_value_percent_rank_plus_currency_count_tier_value) OVER w_part AS plan_stddev_samp_tier_value_percent_rank_plus_currency_count_tier_value,
        MAX(tier_value) OVER w_part AS plan_max_tier_value
    FROM cleaned
    WINDOW w_part AS (PARTITION BY plan, currency)
),
summary AS (
    -- Grouped metrics
    SELECT
        currency,
        CASE WHEN tier_value_cume_dist >= 0.61 THEN 'HIGH' ELSE 'LOW' END AS tier_value_cume_dist_tier,
        ROUND(AVG(tier_value_cume_dist), 2) AS avg_tier_value_cume_dist,
        ROUND(SUM(plan_max_tier_value_percent_rank_plus_currency_count_tier_value) / NULLIF(COUNT(CASE WHEN currency = 'THB' THEN 1 END), 0), 2) AS plan_max_tier_value_percent_rank_plus_currency_count_tier_value_per_thb,
        PERCENTILE_APPROX(tier_value_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS tier_value_cume_dist_quartiles,
        PERCENTILE_APPROX(plan_stddev_samp_tier_value_percent_rank_plus_currency_count_tier_value, 0.9) AS plan_stddev_samp_tier_value_percent_rank_plus_currency_count_tier_value_p90,
        ROUND(AVG(tier_value_cume_dist), 2) AS avg_tier_value_cume_dist_2,
        COUNT(CASE WHEN currency = 'EUR' THEN 1 END) AS eur_count,
        GROUPING(currency) AS grouping_level
    FROM with_windows_price_tiers
    GROUP BY CUBE (currency, CASE WHEN tier_value_cume_dist >= 0.61 THEN 'HIGH' ELSE 'LOW' END)
    HAVING PERCENTILE_APPROX(plan_stddev_samp_tier_value_percent_rank_plus_currency_count_tier_value, 0.9) >= 0
)
SELECT
    tier_value_cume_dist_tier,
    avg_tier_value_cume_dist,
    plan_max_tier_value_percent_rank_plus_currency_count_tier_value_per_thb,
    tier_value_cume_dist_quartiles,
    avg_tier_value_cume_dist_2,
    grouping_level
FROM summary;
