WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) e AS element
)
-- Ranking and gap features
SELECT
    *,
    DENSE_RANK() OVER (PARTITION BY plan ORDER BY element) AS element_dense_rank,
    MIN(element) OVER w_part AS plan_min_element,
    MAX(element) OVER w_part AS plan_max_element
FROM lines
WHERE plan = 'pro'
WINDOW w_part AS (PARTITION BY plan)
