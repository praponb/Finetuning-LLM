WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) x AS tier_value
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(tier_value * 0.85, 2) AS adjusted_tier_value,
        TRIM(plan) AS clean_plan
    FROM lines
    WHERE tier_value <= 72
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY tier_value NULLS LAST) AS tier_value_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY adjusted_tier_value DESC NULLS FIRST) AS adjusted_tier_value_percent_rank,
        SUM(adjusted_tier_value) OVER (PARTITION BY plan) AS plan_sum_adjusted_tier_value
    FROM cleaned
),
cleaned_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        tier_value,
        adjusted_tier_value,
        clean_plan,
        adjusted_tier_value_percent_rank,
        plan_sum_adjusted_tier_value,
        ROUND(adjusted_tier_value_percent_rank * 100, 2) AS adjusted_adjusted_tier_value_percent_rank
    FROM scored
    WHERE tier_value_dense_rank BETWEEN 6 AND 8
)
SELECT
    id,
    tier_value,
    adjusted_tier_value,
    adjusted_tier_value_percent_rank
FROM cleaned_price_tiers
ORDER BY adjusted_tier_value;
