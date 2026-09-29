WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE(tier_limits) AS val
    FROM price_tiers
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        effective_from,
        val,
        MAX(val) OVER w_part AS plan_max_val,
        MIN(val) OVER w_part AS plan_min_val,
        PERCENT_RANK() OVER (ORDER BY val) AS val_percent_rank
    FROM exploded
    WINDOW w_part AS (PARTITION BY plan)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        plan,
        val,
        plan_min_val,
        TRIM(plan) AS clean_plan,
        DATEDIFF(CURRENT_DATE(), effective_from) AS days_since_effective_from
    FROM with_windows
    WHERE effective_from > DATE '2024-05-09'
),
matched_keys AS (
    -- Set difference on keys
    SELECT plan, id
    FROM valid
    EXCEPT
    SELECT plan, id
    FROM price_tiers
    WHERE plan IN ('pro', 'enterprise')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    JOIN matched_keys k ON s.plan = k.plan AND s.id = k.id
),
with_windows_price_tiers AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        val,
        plan_min_val,
        clean_plan,
        days_since_effective_from,
        DENSE_RANK() OVER w_part AS plan_min_val_dense_rank,
        RANK() OVER (PARTITION BY plan ORDER BY plan_min_val DESC NULLS LAST) AS plan_min_val_rank,
        CUME_DIST() OVER w_part_2 AS plan_min_val_cume_dist
    FROM verified
    WINDOW w_part AS (ORDER BY plan_min_val DESC NULLS FIRST), w_part_2 AS (PARTITION BY plan ORDER BY plan_min_val NULLS LAST)
)
-- Ranking and gap features
SELECT
    *,
    RANK() OVER (ORDER BY plan_min_val_dense_rank DESC NULLS FIRST) AS plan_min_val_dense_rank_rank,
    CUME_DIST() OVER (PARTITION BY plan ORDER BY plan_min_val NULLS LAST) AS plan_min_val_cume_dist_2,
    ROUND(plan_min_val_dense_rank / SUM(plan_min_val_dense_rank) OVER (PARTITION BY plan), 4) AS plan_min_val_dense_rank_share,
    CUME_DIST() OVER (ORDER BY val DESC) AS val_cume_dist,
    RANK() OVER w_part AS plan_min_val_rank_2
FROM with_windows_price_tiers
WINDOW w_part AS (PARTITION BY plan ORDER BY plan_min_val DESC)
