WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) li AS tier_value
)
-- Window metrics over several different partitions
SELECT
    id,
    plan,
    CUME_DIST() OVER (ORDER BY tier_value DESC) AS tier_value_cume_dist,
    PERCENT_RANK() OVER (PARTITION BY plan ORDER BY tier_value DESC NULLS FIRST) AS tier_value_percent_rank,
    RANK() OVER (PARTITION BY plan ORDER BY tier_value DESC NULLS LAST) AS tier_value_rank,
    MAX(tier_value) OVER (PARTITION BY plan) AS plan_max_tier_value
FROM lines
WHERE plan = 'enterprise';
