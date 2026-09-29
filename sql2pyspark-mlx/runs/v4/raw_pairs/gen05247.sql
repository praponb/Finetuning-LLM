WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        EXPLODE(tier_prices) AS tier_value
    FROM price_tiers
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(tier_value) OVER (PARTITION BY currency) AS currency_min_tier_value,
        DENSE_RANK() OVER (ORDER BY tier_value DESC NULLS FIRST) AS tier_value_dense_rank,
        STDDEV_SAMP(tier_value) OVER w_recent AS plan_stddev_samp_tier_value,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY tier_value NULLS LAST) AS tier_value_percent_rank
    FROM exploded
    WINDOW w_recent AS (PARTITION BY plan)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        currency,
        tier_value,
        currency_min_tier_value,
        tier_value_dense_rank,
        plan_stddev_samp_tier_value,
        tier_value_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY plan_stddev_samp_tier_value DESC NULLS FIRST) AS plan_stddev_samp_tier_value_percent_rank,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value_percent_rank) AS tier_value_percent_rank_cume_dist,
        CUME_DIST() OVER (ORDER BY currency_min_tier_value NULLS LAST) AS currency_min_tier_value_cume_dist
    FROM enriched
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN plan_stddev_samp_tier_value >= 87 THEN 'High' WHEN plan_stddev_samp_tier_value >= 21 THEN 'Medium' ELSE 'Low' END AS plan_stddev_samp_tier_value_band
    FROM scored
    WHERE tier_value_dense_rank = 8
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN tier_value >= 74 THEN 'HIGH' ELSE 'LOW' END AS tier_value_tier,
    CASE WHEN currency_min_tier_value >= 63 THEN 'LARGE' ELSE 'SMALL' END AS currency_min_tier_value_tier,
    SUM(tier_value_percent_rank) FILTER (WHERE plan = 'pro') AS pro_total_tier_value_percent_rank,
    AVG(plan_stddev_samp_tier_value) FILTER (WHERE currency = 'THB') AS thb_avg_plan_stddev_samp_tier_value,
    COUNT(DISTINCT plan) AS distinct_plans
FROM cleaned
GROUP BY CASE WHEN tier_value >= 74 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN currency_min_tier_value >= 63 THEN 'LARGE' ELSE 'SMALL' END;
