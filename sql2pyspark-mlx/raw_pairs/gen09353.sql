WITH exploded AS (
    SELECT
        id,
        plan,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) x AS tier_value
),
base AS (
    SELECT *, LOWER(plan) plan_lower
    FROM exploded
    WHERE plan = 'pro'
),
filtered AS (
    SELECT *, COALESCE(tier_value, 0) tier_value_filled
    FROM base
    WHERE tier_value BETWEEN 35 AND 71
),
remaining_keys AS (
    SELECT id
    FROM filtered
    EXCEPT DISTINCT
    SELECT id
    FROM price_tiers
    WHERE YEAR(effective_from) = 2025
),
verified AS (
    SELECT s.*
    FROM filtered s
    JOIN remaining_keys k ON s.id = k.id
),
stats AS (
    SELECT
        CASE WHEN tier_value_filled >= 74 THEN 'LARGE' ELSE 'SMALL' END AS tier_value_filled_tier,
        SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
        ROUND(SUM(tier_value_filled) / NULLIF(COUNT(CASE WHEN plan = 'enterprise' THEN 1 END), 0), 2) AS tier_value_filled_per_enterprise
    FROM verified
    GROUP BY tier_value_filled_tier
    HAVING ROUND(SUM(tier_value_filled) / NULLIF(COUNT(CASE WHEN plan = 'enterprise' THEN 1 END), 0), 2) > 0
)
SELECT tier_value_filled_tier, plan_list
FROM stats;
