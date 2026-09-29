WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_limits) AS tier_value
    FROM price_tiers
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        plan,
        effective_from,
        tier_value,
        CONCAT(plan, ' ', currency) AS plan_currency
    FROM flattened
    WHERE YEAR(effective_from) = 2024
),
base_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(plan) AS plan_length,
        DATE_TRUNC('month', effective_from) AS effective_from_period
    FROM base
    WHERE tier_value < 44
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        plan,
        effective_from,
        DENSE_RANK() OVER (ORDER BY tier_value DESC NULLS LAST) AS tier_value_dense_rank,
        MIN(plan_length) OVER (PARTITION BY plan) AS plan_min_plan_length,
        STDDEV_SAMP(tier_value) OVER (PARTITION BY DATE_TRUNC('WEEK', effective_from)) AS effective_from_period_stddev_samp_tier_value
    FROM base_price_tiers
),
with_windows_price_tiers AS (
    -- Ranking and gap features
    SELECT
        plan,
        effective_from,
        plan_min_plan_length,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY effective_from_period_stddev_samp_tier_value NULLS LAST) AS effective_from_period_stddev_samp_tier_value_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY effective_from_period_stddev_samp_tier_value DESC NULLS FIRST) AS effective_from_period_stddev_samp_tier_value_percent_rank_2,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY tier_value_dense_rank DESC) AS tier_value_dense_rank_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY MONTH(effective_from), plan ORDER BY effective_from_period_stddev_samp_tier_value NULLS LAST) AS effective_from_period_stddev_samp_tier_value_percent_rank_3
    FROM with_windows
    WHERE plan = 'enterprise'
),
grouped AS (
    -- Grouped metrics
    SELECT
        CASE WHEN plan_min_plan_length >= 61 THEN 'PREMIUM' ELSE 'STANDARD' END AS plan_min_plan_length_tier,
        COUNT(*) AS num_rows,
        AVG(effective_from_period_stddev_samp_tier_value_percent_rank_2) FILTER (WHERE plan = 'basic') AS basic_avg_effective_from_period_stddev_samp_tier_value_percent_rank_2,
        COUNT(*) AS event_count,
        SUM(CASE WHEN plan = 'pro' THEN effective_from_period_stddev_samp_tier_value_percent_rank_2 ELSE 0 END) AS pro_effective_from_period_stddev_samp_tier_value_percent_rank_2,
        COUNT(CASE WHEN plan = 'enterprise' THEN 1 END) AS enterprise_count
    FROM with_windows_price_tiers
    GROUP BY 1
    HAVING pro_effective_from_period_stddev_samp_tier_value_percent_rank_2 > 0
)
SELECT
    plan_min_plan_length_tier,
    num_rows,
    basic_avg_effective_from_period_stddev_samp_tier_value_percent_rank_2,
    event_count,
    pro_effective_from_period_stddev_samp_tier_value_percent_rank_2,
    enterprise_count
FROM grouped
ORDER BY num_rows DESC, pro_effective_from_period_stddev_samp_tier_value_percent_rank_2
