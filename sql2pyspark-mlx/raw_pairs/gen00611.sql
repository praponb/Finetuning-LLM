WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) t AS val
)
-- Multi-level aggregation
SELECT
    CASE WHEN val >= 61 THEN 'LARGE' ELSE 'SMALL' END AS val_tier,
    plan,
    SUM(val) AS total_val,
    SUM(CASE WHEN plan = 'starter' THEN val ELSE 0 END) AS starter_val,
    RANK() OVER (PARTITION BY CASE WHEN val >= 61 THEN 'LARGE' ELSE 'SMALL' END ORDER BY SUM(CASE WHEN plan = 'starter' THEN val ELSE 0 END) DESC) AS starter_val_rank
FROM flattened
GROUP BY val_tier, plan
ORDER BY val_tier;
