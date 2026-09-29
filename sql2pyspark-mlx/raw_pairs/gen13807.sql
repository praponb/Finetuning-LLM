WITH lines AS (
    SELECT
        id,
        plan,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) li AS val
),
cleaned AS (
    SELECT
        *,
        LOWER(currency) AS currency_lower,
        UPPER(plan) AS plan_upper
    FROM lines
    WHERE val BETWEEN 47 AND 65
),
enriched AS (
    SELECT
        *,
        STDDEV_SAMP(val) OVER (PARTITION BY currency, plan) AS currency_stddev_samp_val,
        RANK() OVER (ORDER BY val NULLS LAST) AS val_rank,
        MIN(val) OVER (PARTITION BY plan, currency) AS plan_min_val
    FROM cleaned
    WHERE plan = 'starter'
),
cleaned_price_tiers AS (
    SELECT *, plan_min_val - val AS plan_min_val_minus_val
    FROM enriched
    WHERE val <= 28
)
SELECT
    currency,
    metric_name,
    value
FROM (
    SELECT
        currency,
        val,
        plan_min_val_minus_val
    FROM cleaned_price_tiers
) src
UNPIVOT (
    value FOR metric_name IN (val, plan_min_val_minus_val)
) long_rows
