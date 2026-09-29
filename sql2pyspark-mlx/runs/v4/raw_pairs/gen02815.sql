WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) li AS val
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY val ASC NULLS FIRST) val_dense_rank,
        RANK() OVER (PARTITION BY plan ORDER BY val DESC NULLS LAST) val_rank
    FROM exploded
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        plan,
        effective_from,
        val,
        val_dense_rank,
        UPPER(plan) plan_upper
    FROM metrics
    WHERE val BETWEEN 26 AND 41
),
kept_keys AS (
    -- Set intersection on keys
    SELECT plan
    FROM valid
    INTERSECT ALL
    SELECT plan
    FROM price_tiers
    WHERE plan = 'basic'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    JOIN kept_keys k ON s.plan = k.plan
)
SELECT
    plan,
    effective_from,
    val_dense_rank,
    plan_upper,
    CASE WHEN plan RLIKE '^(pro|starter)$' THEN 'EXTENDED' WHEN plan RLIKE '^(basic|enterprise)$' THEN 'CORE' ELSE 'GROUP_A' END AS plan_zone
FROM kept_rows
ORDER BY val_dense_rank DESC, plan ASC;
