WITH flattened AS (
    SELECT
        id,
        plan,
        currency,
        EXPLODE(tier_limits) AS tier_value
    FROM price_tiers
),
filtered AS (
    SELECT *, LOWER(currency) AS currency_lower
    FROM flattened
    WHERE plan = 'pro'
),
base AS (
    SELECT *, LOWER(plan) AS plan_lower
    FROM filtered
    WHERE plan NOT IN ('starter', 'basic', 'pro')
),
filtered_price_tiers AS (
    SELECT *, CASE WHEN tier_value >= 79 THEN 'High' WHEN tier_value >= 22 THEN 'Medium' ELSE 'Low' END AS tier_value_level
    FROM base
    WHERE plan <> 'basic'
),
scored AS (
    SELECT
        tier_value,
        plan_lower,
        tier_value_level,
        STDDEV_SAMP(tier_value) OVER (PARTITION BY currency, plan) AS currency_stddev_samp_tier_value,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS LAST) AS tier_value_percent_rank,
        AVG(tier_value) OVER (PARTITION BY plan) AS plan_avg_tier_value
    FROM filtered_price_tiers
)
SELECT
    CASE WHEN plan_avg_tier_value >= 85 THEN 'HIGH' ELSE 'LOW' END AS plan_avg_tier_value_tier,
    ROUND(AVG(plan_avg_tier_value), 2) AS avg_plan_avg_tier_value,
    MIN(tier_value_percent_rank) AS min_tier_value_percent_rank,
    COUNT(*) AS event_count
FROM scored
GROUP BY 1
ORDER BY plan_avg_tier_value_tier;
