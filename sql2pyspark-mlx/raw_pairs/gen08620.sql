WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        EXPLODE_OUTER(tier_prices) AS element
    FROM price_tiers
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(element, 0) AS element_filled,
        TRIM(plan) AS clean_plan
    FROM lines
    WHERE element < 76
),
valid_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        element,
        clean_plan,
        CAST(element_filled AS INT) AS element_filled_int,
        element_filled / NULLIF(element, 0) AS element_filled_per_element
    FROM valid
    WHERE plan != 'starter'
),
valid_price_tiers_2 AS (
    -- Keep recent, valid rows
    SELECT *, element_filled_int + element_filled_per_element AS element_filled_int_plus_element_filled_per_element
    FROM valid_price_tiers
    WHERE plan = 'basic'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER w_grp AS element_filled_int_plus_element_filled_per_element_percent_rank,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY element_filled_int ASC NULLS FIRST) AS element_filled_int_cume_dist,
        MIN(element_filled_int_plus_element_filled_per_element) OVER w_grp_2 AS plan_min_element_filled_int_plus_element_filled_per_element,
        RANK() OVER w_running AS element_filled_per_element_rank
    FROM valid_price_tiers_2
    WHERE plan = 'enterprise'
    WINDOW w_grp AS (ORDER BY element_filled_int_plus_element_filled_per_element DESC NULLS LAST), w_grp_2 AS (PARTITION BY plan), w_running AS (PARTITION BY plan ORDER BY element_filled_per_element ASC)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        element,
        clean_plan,
        element_filled_int_plus_element_filled_per_element,
        element_filled_int_plus_element_filled_per_element_percent_rank,
        plan_min_element_filled_int_plus_element_filled_per_element,
        element_filled_per_element_rank,
        RANK() OVER w_hist AS element_filled_per_element_rank_2
    FROM with_windows
    WINDOW w_hist AS (ORDER BY element_filled_per_element DESC NULLS LAST)
)
SELECT
    element,
    clean_plan,
    element_filled_int_plus_element_filled_per_element_percent_rank,
    plan_min_element_filled_int_plus_element_filled_per_element,
    element_filled_per_element_rank
FROM metrics
