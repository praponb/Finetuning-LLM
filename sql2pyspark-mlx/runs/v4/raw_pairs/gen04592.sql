WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        EXPLODE(tier_prices) AS val
    FROM price_tiers
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, plan
    FROM exploded
    EXCEPT DISTINCT
    SELECT id, plan
    FROM price_tiers
    WHERE currency IN ('THB', 'USD', 'EUR')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM exploded s
    JOIN remaining_keys k ON s.id = k.id AND s.plan = k.plan
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY val DESC NULLS LAST) AS val_dense_rank,
        DENSE_RANK() OVER (ORDER BY val DESC) AS val_dense_rank_2,
        COUNT(val) OVER (PARTITION BY plan) AS plan_count_val
    FROM verified
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(val_dense_rank_2, 2) AS val_dense_rank_2_rounded,
        CASE WHEN plan IN ('basic', 'pro') THEN 'Core' ELSE 'Other' END AS plan_group
    FROM with_windows
    WHERE val < 39
)
SELECT
    id,
    plan,
    val,
    val_dense_rank,
    val_dense_rank_2,
    plan_count_val,
    plan_group,
    CASE WHEN plan RLIKE '^(starter|enterprise)$' THEN 'EXTENDED' ELSE 'CORE' END AS plan_zone
FROM valid
ORDER BY val_dense_rank NULLS FIRST
