WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        EXPLODE(tier_prices) AS tier_value
    FROM price_tiers
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN plan = 'starter' THEN 1 ELSE 0 END AS is_starter
    FROM exploded
    WHERE tier_value < 70
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, LEAST(tier_value, is_starter) AS least_tier_value_is_starter
    FROM valid
    WHERE tier_value < 61
),
cleaned_price_tiers AS (
    -- Keep recent, valid rows
    SELECT *, TRIM(plan) AS clean_plan
    FROM cleaned
    WHERE tier_value >= 36
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        tier_value,
        is_starter,
        AVG(tier_value) OVER (PARTITION BY plan) AS plan_avg_tier_value
    FROM cleaned_price_tiers
    WHERE plan NOT IN ('enterprise', 'basic')
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY tier_value DESC NULLS LAST) AS tier_value_dense_rank,
        DENSE_RANK() OVER (ORDER BY plan_avg_tier_value NULLS LAST) AS plan_avg_tier_value_dense_rank
    FROM scored
),
scored_price_tiers AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER w_ordered AS is_starter_rank,
        RANK() OVER (ORDER BY is_starter DESC) AS is_starter_rank_2,
        PERCENT_RANK() OVER (ORDER BY plan_avg_tier_value NULLS FIRST) AS plan_avg_tier_value_percent_rank
    FROM enriched
    WINDOW w_ordered AS (ORDER BY is_starter NULLS FIRST)
)
-- Multi-level aggregation
SELECT
    CASE WHEN tier_value >= 73 THEN 'LARGE' ELSE 'SMALL' END AS tier_value_tier,
    AVG(is_starter) AS avg_is_starter,
    AVG(is_starter_rank) AS avg_is_starter_rank,
    APPROX_PERCENTILE(is_starter, ARRAY(0.25, 0.5, 0.75)) AS is_starter_quartiles
FROM scored_price_tiers
GROUP BY CASE WHEN tier_value >= 73 THEN 'LARGE' ELSE 'SMALL' END
HAVING avg_is_starter_rank >= 0;
