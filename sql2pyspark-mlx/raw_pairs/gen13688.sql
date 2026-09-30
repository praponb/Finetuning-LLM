WITH exploded AS (
    SELECT
        id,
        plan,
        effective_from,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) e AS val
),
metrics AS (
    SELECT
        *,
        MAX(val) OVER (PARTITION BY MONTH(effective_from), plan) AS effective_from_month_max_val,
        SUM(val) OVER (PARTITION BY plan) AS plan_sum_val
    FROM exploded
),
metrics_price_tiers AS (
    SELECT
        id,
        plan_sum_val,
        RANK() OVER (PARTITION BY plan ORDER BY effective_from_month_max_val DESC) AS effective_from_month_max_val_rank
    FROM metrics
),
base AS (
    SELECT *, plan_sum_val - effective_from_month_max_val_rank AS plan_sum_val_minus_effective_from_month_max_val_rank
    FROM metrics_price_tiers
    WHERE effective_from_month_max_val_rank BETWEEN 3 AND 5
),
valid AS (
    SELECT *, CASE WHEN plan_sum_val >= 316 THEN 'High' WHEN plan_sum_val >= 169 THEN 'Medium' ELSE 'Low' END AS plan_sum_val_tier
    FROM base
    WHERE plan_sum_val <= 305
)
SELECT
    id,
    plan_sum_val,
    effective_from_month_max_val_rank
FROM valid
