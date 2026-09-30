WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) li AS val
),
with_windows AS (
    -- Ranking and gap features
    SELECT *, SUM(val) OVER (PARTITION BY plan) AS plan_sum_val
    FROM exploded
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        currency,
        MAX(val) OVER (PARTITION BY currency, plan) AS currency_max_val,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY plan_sum_val DESC NULLS LAST) AS plan_sum_val_cume_dist,
        RANK() OVER w_ordered AS plan_sum_val_rank
    FROM with_windows
    WINDOW w_ordered AS (PARTITION BY currency ORDER BY plan_sum_val DESC)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        currency_max_val,
        plan_sum_val_rank,
        CONCAT_WS(',', plan, currency) AS plan_currency_key,
        ROUND(plan_sum_val_rank, 2) AS plan_sum_val_rank_rounded
    FROM metrics
    WHERE currency IN ('THB', 'USD', 'EUR')
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        plan,
        CASE WHEN plan_sum_val_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS plan_sum_val_rank_tier,
        ROUND(AVG(plan_sum_val_rank_rounded), 2) AS avg_plan_sum_val_rank_rounded,
        COUNT_IF(plan = 'pro') AS pro_events,
        SIZE(COLLECT_SET(plan)) AS num_plans,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY COUNT_IF(plan = 'pro') DESC) AS pro_events_dense_rank
    FROM cleaned
    GROUP BY plan, CASE WHEN plan_sum_val_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    plan,
    plan_sum_val_rank_tier,
    pro_events_dense_rank
FROM grouped
