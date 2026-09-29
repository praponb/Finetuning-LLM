WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) t AS element
)
-- Running and moving aggregates
SELECT
    id,
    plan,
    currency,
    element,
    MAX(element) OVER (PARTITION BY plan) plan_max_element,
    PERCENT_RANK() OVER (PARTITION BY currency, plan ORDER BY element ASC) element_percent_rank,
    DENSE_RANK() OVER (PARTITION BY currency ORDER BY element DESC) element_dense_rank
FROM lines
