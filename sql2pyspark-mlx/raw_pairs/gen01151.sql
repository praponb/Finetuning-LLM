WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        EXPLODE(tier_limits) AS tier_value
    FROM price_tiers
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        tier_value,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY tier_value ASC NULLS LAST) AS tier_value_percent_rank,
        STDDEV_SAMP(tier_value) OVER (PARTITION BY currency) AS currency_stddev_samp_tier_value,
        MIN(tier_value) OVER (PARTITION BY plan) AS plan_min_tier_value
    FROM flattened
    WHERE plan != 'basic'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        tier_value_percent_rank,
        plan_min_tier_value,
        RANK() OVER (PARTITION BY plan ORDER BY tier_value_percent_rank ASC NULLS FIRST) AS tier_value_percent_rank_rank,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY plan_min_tier_value DESC NULLS LAST) AS plan_min_tier_value_percent_rank
    FROM metrics
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        tier_value_percent_rank,
        plan_min_tier_value,
        tier_value_percent_rank_rank,
        plan_min_tier_value_percent_rank,
        ROUND(tier_value_percent_rank / tier_value_percent_rank_rank, 2) AS tier_value_percent_rank_per_tier_value_percent_rank_rank,
        COALESCE(tier_value_percent_rank, 0) AS tier_value_percent_rank_filled
    FROM with_windows
    WHERE tier_value_percent_rank >= 0.79
)
-- Grouped metrics
SELECT
    CASE WHEN tier_value_percent_rank >= 0.7 THEN 'HIGH' ELSE 'LOW' END AS tier_value_percent_rank_tier,
    MAX(tier_value_percent_rank_rank) AS max_tier_value_percent_rank_rank,
    AVG(tier_value_percent_rank_filled) AS avg_tier_value_percent_rank_filled,
    APPROX_PERCENTILE(tier_value_percent_rank, 0.95) AS tier_value_percent_rank_p95
FROM valid
GROUP BY 1
ORDER BY tier_value_percent_rank_p95 DESC;
