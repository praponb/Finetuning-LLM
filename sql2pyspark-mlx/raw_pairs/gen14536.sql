WITH exploded AS (
    SELECT
        id,
        plan,
        currency,
        EXPLODE(tier_limits) AS element
    FROM price_tiers
)
SELECT
    plan,
    currency,
    STDDEV_SAMP(element) OVER w_part AS plan_stddev_samp_element,
    SUM(element) OVER w_part AS plan_sum_element
FROM exploded
WHERE element >= 77
WINDOW w_part AS (PARTITION BY plan);
