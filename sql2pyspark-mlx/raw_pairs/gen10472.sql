WITH lines AS (
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE(discounts) AS tier_value
    FROM price_tiers
),
enriched AS (
    SELECT
        *,
        RANK() OVER (ORDER BY tier_value NULLS FIRST) AS tier_value_rank,
        AVG(tier_value) OVER (PARTITION BY plan) AS plan_avg_tier_value,
        RANK() OVER (PARTITION BY plan ORDER BY tier_value DESC NULLS LAST) AS tier_value_rank_2,
        STDDEV_SAMP(tier_value) OVER (PARTITION BY MONTH(effective_from), plan) AS effective_from_month_stddev_samp_tier_value
    FROM lines
),
scored AS (
    SELECT
        id,
        plan,
        tier_value,
        tier_value_rank_2,
        ROUND(tier_value_rank / SUM(tier_value_rank) OVER (PARTITION BY YEAR(effective_from), plan), 4) AS tier_value_rank_share
    FROM enriched
    WHERE effective_from >= '2025-04-14'
),
filtered AS (
    SELECT
        plan,
        tier_value,
        tier_value_rank_2,
        CASE WHEN plan IN ('enterprise', 'starter') THEN 'Group A' ELSE 'Other' END AS plan_group
    FROM scored
    WHERE plan IN ('pro', 'enterprise', 'basic')
)
SELECT
    plan,
    CASE WHEN tier_value >= 63 THEN 'LARGE' ELSE 'SMALL' END AS tier_value_tier,
    APPROX_PERCENTILE(tier_value_rank_2, 0.9) AS tier_value_rank_2_p90,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    GROUPING(plan) AS gid
FROM filtered
GROUP BY CUBE (plan, CASE WHEN tier_value >= 63 THEN 'LARGE' ELSE 'SMALL' END)
HAVING APPROX_PERCENTILE(tier_value_rank_2, 0.9) < 1000000
