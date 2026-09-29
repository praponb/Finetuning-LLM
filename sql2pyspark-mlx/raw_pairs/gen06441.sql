WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) li AS tier_value
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY YEAR(effective_from) ORDER BY tier_value DESC NULLS FIRST) AS tier_value_percent_rank,
        STDDEV_SAMP(tier_value) OVER (PARTITION BY YEAR(effective_from), plan) AS effective_from_year_stddev_samp_tier_value,
        PERCENT_RANK() OVER (ORDER BY tier_value) AS tier_value_percent_rank_2
    FROM flattened
    WHERE effective_from < '2024-09-17'
),
enriched_price_tiers AS (
    -- Ranking and gap features
    SELECT
        plan,
        tier_value_percent_rank,
        effective_from_year_stddev_samp_tier_value,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY effective_from_year_stddev_samp_tier_value DESC) AS effective_from_year_stddev_samp_tier_value_percent_rank
    FROM enriched
),
scored AS (
    -- Running and moving aggregates
    SELECT
        plan,
        tier_value_percent_rank,
        AVG(effective_from_year_stddev_samp_tier_value) OVER w_part AS plan_avg_effective_from_year_stddev_samp_tier_value,
        RANK() OVER w_grp AS effective_from_year_stddev_samp_tier_value_percent_rank_rank,
        RANK() OVER (PARTITION BY plan ORDER BY effective_from_year_stddev_samp_tier_value DESC NULLS FIRST) AS effective_from_year_stddev_samp_tier_value_rank,
        MAX(effective_from_year_stddev_samp_tier_value_percent_rank) OVER w_part AS plan_max_effective_from_year_stddev_samp_tier_value_percent_rank
    FROM enriched_price_tiers
    WINDOW w_part AS (PARTITION BY plan), w_grp AS (PARTITION BY plan ORDER BY effective_from_year_stddev_samp_tier_value_percent_rank DESC NULLS LAST)
)
-- Multi-level aggregation
SELECT
    CASE WHEN plan_avg_effective_from_year_stddev_samp_tier_value >= 72 THEN 'LARGE' ELSE 'SMALL' END AS plan_avg_effective_from_year_stddev_samp_tier_value_tier,
    plan,
    SUM(CASE WHEN plan = 'basic' THEN plan_max_effective_from_year_stddev_samp_tier_value_percent_rank ELSE 0 END) AS basic_plan_max_effective_from_year_stddev_samp_tier_value_percent_rank,
    COUNT_IF(plan = 'starter') AS starter_events,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    COUNT(DISTINCT plan) AS distinct_plans,
    APPROX_PERCENTILE(tier_value_percent_rank, 0.5) AS tier_value_percent_rank_p50,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list
FROM scored
GROUP BY plan_avg_effective_from_year_stddev_samp_tier_value_tier, plan
ORDER BY tier_value_percent_rank_p50 NULLS LAST;
