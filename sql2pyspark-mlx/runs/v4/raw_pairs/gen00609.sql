WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        EXPLODE(tier_prices) AS val
    FROM price_tiers
)
-- Running and moving aggregates
SELECT
    *,
    STDDEV_SAMP(val) OVER (PARTITION BY plan) AS plan_stddev_samp_val,
    STDDEV_SAMP(val) OVER (PARTITION BY plan) AS plan_stddev_samp_val_2,
    CUME_DIST() OVER (PARTITION BY plan ORDER BY val DESC NULLS LAST) AS val_cume_dist
FROM exploded
