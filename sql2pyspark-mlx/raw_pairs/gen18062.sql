WITH exploded AS (
    SELECT
        id,
        plan,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) t AS val
),
valid AS (
    SELECT *, ROUND(val * 1.1, 2) AS val_scaled
    FROM exploded
    WHERE currency = 'THB'
),
base AS (
    SELECT *, val_scaled + val AS val_scaled_plus_val
    FROM valid
    WHERE currency = 'THB'
),
base_price_tiers AS (
    SELECT *, INITCAP(TRIM(plan)) AS plan_title
    FROM base
    WHERE val_scaled BETWEEN 61 AND 74
),
with_windows AS (
    SELECT
        id,
        currency,
        val,
        val_scaled,
        val_scaled_plus_val,
        plan_title,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY val_scaled_plus_val ASC NULLS FIRST) AS val_scaled_plus_val_dense_rank,
        CUME_DIST() OVER (ORDER BY val ASC NULLS LAST) AS val_cume_dist
    FROM base_price_tiers
    WHERE val_scaled > 44
),
base_price_tiers_2 AS (
    SELECT *, GREATEST(val, val_scaled) AS greatest_val_val_scaled
    FROM with_windows
    WHERE val_scaled_plus_val_dense_rank = 5
)
SELECT
    id,
    val,
    val_scaled,
    val_scaled_plus_val,
    plan_title,
    val_scaled_plus_val_dense_rank,
    greatest_val_val_scaled
FROM base_price_tiers_2
ORDER BY id DESC NULLS LAST
