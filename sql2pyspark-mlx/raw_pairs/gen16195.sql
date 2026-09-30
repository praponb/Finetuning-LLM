WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) t AS tier_value
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(tier_value, 0) AS tier_value_filled
    FROM flattened
    WHERE plan IN ('basic', 'pro', 'starter')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        SUM(tier_value_filled) OVER (PARTITION BY DATE_TRUNC('WEEK', effective_from), plan) AS effective_from_period_sum_tier_value_filled,
        MIN(tier_value) OVER (PARTITION BY plan) AS plan_min_tier_value
    FROM filtered
),
remaining_keys AS (
    -- Set difference on keys
    SELECT plan
    FROM with_windows
    EXCEPT ALL
    SELECT plan
    FROM price_tiers
    WHERE currency IN ('USD', 'EUR')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    JOIN remaining_keys k ON s.plan = k.plan
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, plan
    FROM reconciled
    INTERSECT ALL
    SELECT id, plan
    FROM price_tiers
    WHERE effective_from <= '2023-05-17'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM reconciled s
    JOIN matched_keys k ON s.id = k.id AND s.plan = k.plan
)
SELECT
    id,
    effective_from_period_sum_tier_value_filled,
    plan_min_tier_value
FROM verified
