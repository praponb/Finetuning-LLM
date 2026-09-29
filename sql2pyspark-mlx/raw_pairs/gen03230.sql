WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) x AS val
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan,
        AVG(val) OVER (PARTITION BY MONTH(effective_from), plan) AS effective_from_month_avg_val,
        PERCENT_RANK() OVER (ORDER BY val DESC NULLS LAST) AS val_percent_rank,
        STDDEV_SAMP(val) OVER (PARTITION BY DATE_TRUNC('WEEK', effective_from), plan) AS effective_from_period_stddev_samp_val,
        CUME_DIST() OVER (ORDER BY val DESC) AS val_cume_dist,
        STDDEV_SAMP(val) OVER (PARTITION BY plan) AS plan_stddev_samp_val
    FROM exploded
    WHERE effective_from > '2025-08-04'
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        effective_from_month_avg_val,
        val_percent_rank,
        effective_from_period_stddev_samp_val,
        val_cume_dist,
        UPPER(plan) AS plan_upper
    FROM with_windows
    WHERE effective_from_month_avg_val BETWEEN 40 AND 60
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(val_percent_rank * 1.07, 2) AS adjusted_val_percent_rank
    FROM base
    WHERE val_cume_dist >= 0.45
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        val_percent_rank,
        effective_from_period_stddev_samp_val,
        val_cume_dist,
        adjusted_val_percent_rank,
        SUM(effective_from_period_stddev_samp_val) OVER (PARTITION BY plan) AS plan_sum_effective_from_period_stddev_samp_val,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY effective_from_month_avg_val DESC NULLS LAST) AS effective_from_month_avg_val_cume_dist
    FROM filtered
),
base_price_tiers AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(plan_sum_effective_from_period_stddev_samp_val * 0.9, 2) AS plan_sum_effective_from_period_stddev_samp_val_adjusted
    FROM metrics
    WHERE effective_from_period_stddev_samp_val >= 23
)
SELECT
    effective_from_period_stddev_samp_val,
    val_cume_dist,
    effective_from_month_avg_val_cume_dist,
    plan_sum_effective_from_period_stddev_samp_val_adjusted
FROM base_price_tiers
ORDER BY plan_sum_effective_from_period_stddev_samp_val_adjusted DESC NULLS LAST;
