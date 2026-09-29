WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) x AS tier_value
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        plan,
        currency,
        MIN(tier_value) OVER w_recent AS currency_min_tier_value,
        COUNT(tier_value) OVER (PARTITION BY plan, currency) AS plan_count_tier_value,
        DENSE_RANK() OVER w_part AS tier_value_dense_rank
    FROM lines
    WINDOW w_recent AS (PARTITION BY currency), w_part AS (ORDER BY tier_value DESC NULLS FIRST)
),
enriched_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY currency_min_tier_value DESC NULLS FIRST) AS currency_min_tier_value_dense_rank,
        RANK() OVER (PARTITION BY currency ORDER BY currency_min_tier_value NULLS FIRST) AS currency_min_tier_value_rank
    FROM enriched
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        plan,
        currency,
        currency_min_tier_value,
        plan_count_tier_value,
        tier_value_dense_rank,
        currency_min_tier_value_dense_rank,
        currency_min_tier_value_rank,
        ROUND(currency_min_tier_value_rank, 0) AS currency_min_tier_value_rank_rounded
    FROM enriched_price_tiers
    WHERE currency_min_tier_value >= 39
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        currency,
        plan_count_tier_value,
        currency_min_tier_value_dense_rank,
        currency_min_tier_value_rank_rounded,
        currency_min_tier_value_rank + tier_value_dense_rank AS currency_min_tier_value_rank_plus_tier_value_dense_rank,
        ROUND(tier_value_dense_rank, 1) AS tier_value_dense_rank_rounded
    FROM filtered
    WHERE currency_min_tier_value_dense_rank = 6
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN currency_min_tier_value_rank_plus_tier_value_dense_rank >= 74 THEN 'HIGH' ELSE 'LOW' END AS currency_min_tier_value_rank_plus_tier_value_dense_rank_tier,
        CASE WHEN tier_value_dense_rank_rounded >= 74 THEN 'HIGH' ELSE 'LOW' END AS tier_value_dense_rank_rounded_tier,
        ROUND(AVG(currency_min_tier_value_rank_rounded), 2) AS avg_currency_min_tier_value_rank_rounded,
        ROUND(AVG(currency_min_tier_value_rank_rounded), 2) AS avg_currency_min_tier_value_rank_rounded_2,
        COUNT(CASE WHEN currency = 'EUR' THEN 1 END) AS eur_count,
        APPROX_PERCENTILE(currency_min_tier_value_rank_rounded, ARRAY(0.25, 0.5, 0.75)) AS currency_min_tier_value_rank_rounded_quartiles,
        GROUPING_ID() AS grouping_level
    FROM valid
    GROUP BY GROUPING SETS ((CASE WHEN currency_min_tier_value_rank_plus_tier_value_dense_rank >= 74 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN tier_value_dense_rank_rounded >= 74 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN currency_min_tier_value_rank_plus_tier_value_dense_rank >= 74 THEN 'HIGH' ELSE 'LOW' END))
    HAVING avg_currency_min_tier_value_rank_rounded < 1000000
)
SELECT
    currency_min_tier_value_rank_plus_tier_value_dense_rank_tier,
    tier_value_dense_rank_rounded_tier,
    avg_currency_min_tier_value_rank_rounded_2,
    eur_count,
    currency_min_tier_value_rank_rounded_quartiles,
    grouping_level
FROM grouped
ORDER BY avg_currency_min_tier_value_rank_rounded_2 DESC, currency_min_tier_value_rank_plus_tier_value_dense_rank_tier;
