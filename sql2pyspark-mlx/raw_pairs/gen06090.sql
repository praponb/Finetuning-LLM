WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) li AS tier_value
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM flattened
    EXCEPT
    SELECT id
    FROM price_tiers
    WHERE currency <> 'THB'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM flattened s
    INNER JOIN kept_keys k ON s.id = k.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (ORDER BY tier_value NULLS FIRST) AS tier_value_rank,
        COUNT(tier_value) OVER w_hist AS effective_from_period_count_tier_value,
        SUM(tier_value) OVER (PARTITION BY DATE_TRUNC('week', effective_from)) AS effective_from_period_sum_tier_value,
        MAX(tier_value) OVER (PARTITION BY plan) AS plan_max_tier_value,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY tier_value NULLS FIRST) AS tier_value_percent_rank
    FROM kept_rows
    WHERE tier_value >= 67
    WINDOW w_hist AS (PARTITION BY DATE_TRUNC('WEEK', effective_from), plan)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        tier_value_rank,
        tier_value_percent_rank,
        RANK() OVER w_grp AS tier_value_percent_rank_rank,
        CUME_DIST() OVER (ORDER BY tier_value DESC NULLS LAST) AS tier_value_cume_dist,
        MIN(plan_max_tier_value) OVER (PARTITION BY plan) AS plan_min_plan_max_tier_value
    FROM with_windows
    WINDOW w_grp AS (PARTITION BY plan ORDER BY tier_value_percent_rank DESC NULLS LAST)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        tier_value_rank,
        tier_value_percent_rank_rank,
        DENSE_RANK() OVER w_ordered AS tier_value_percent_rank_rank_dense_rank,
        DENSE_RANK() OVER w_part AS tier_value_rank_dense_rank,
        PERCENT_RANK() OVER w_hist AS tier_value_percent_rank_rank_percent_rank,
        RANK() OVER (ORDER BY tier_value_percent_rank NULLS FIRST) AS tier_value_percent_rank_rank_2,
        PERCENT_RANK() OVER w_ordered AS tier_value_percent_rank_rank_percent_rank_2
    FROM enriched
    WINDOW w_ordered AS (ORDER BY tier_value_percent_rank_rank DESC NULLS LAST), w_part AS (ORDER BY tier_value_rank DESC NULLS LAST), w_hist AS (ORDER BY tier_value_percent_rank_rank DESC)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(tier_value_percent_rank_rank_percent_rank_2 AS INT) AS tier_value_percent_rank_rank_percent_rank_2_int,
        CAST(tier_value_percent_rank_rank_percent_rank_2 AS INT) AS tier_value_percent_rank_rank_percent_rank_2_int_2
    FROM metrics
    WHERE tier_value_percent_rank_rank >= 4
),
with_windows_price_tiers AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER w_grp AS tier_value_percent_rank_rank_percent_rank_2_int_cume_dist,
        PERCENT_RANK() OVER w_part AS tier_value_rank_dense_rank_percent_rank,
        RANK() OVER (ORDER BY tier_value_rank DESC NULLS LAST) AS tier_value_rank_rank,
        RANK() OVER (ORDER BY tier_value_percent_rank_rank_2 DESC) AS tier_value_percent_rank_rank_2_rank,
        RANK() OVER (ORDER BY tier_value_percent_rank_rank_percent_rank NULLS FIRST) AS tier_value_percent_rank_rank_percent_rank_rank
    FROM cleaned
    WINDOW w_grp AS (ORDER BY tier_value_percent_rank_rank_percent_rank_2_int DESC), w_part AS (ORDER BY tier_value_rank_dense_rank DESC)
)
SELECT
    tier_value_percent_rank_rank,
    tier_value_percent_rank_rank_percent_rank,
    tier_value_percent_rank_rank_2,
    tier_value_percent_rank_rank_percent_rank_2_int,
    tier_value_percent_rank_rank_percent_rank_2_int_2,
    tier_value_percent_rank_rank_percent_rank_2_int_cume_dist,
    tier_value_rank_dense_rank_percent_rank,
    tier_value_rank_rank
FROM with_windows_price_tiers
