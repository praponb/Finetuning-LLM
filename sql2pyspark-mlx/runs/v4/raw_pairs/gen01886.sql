WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) t AS val
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        plan,
        effective_from,
        val,
        TRIM(plan) AS clean_plan
    FROM exploded
    WHERE effective_from >= DATE '2024-09-22'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(val AS INT) AS val_int,
        CASE WHEN val >= 67 THEN 'High' WHEN val >= 31 THEN 'Medium' ELSE 'Low' END AS val_band
    FROM cleaned
    WHERE effective_from > DATE '2024-06-06'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY val_int DESC NULLS FIRST) AS val_int_percent_rank,
        COUNT(val) OVER (PARTITION BY plan) AS plan_count_val,
        RANK() OVER (ORDER BY val DESC NULLS FIRST) AS val_rank
    FROM valid
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (ORDER BY val_int DESC NULLS FIRST) AS val_int_rank,
        STDDEV_SAMP(val_int) OVER (PARTITION BY plan) AS plan_stddev_samp_val_int,
        SUM(val_int) OVER (PARTITION BY plan) AS plan_sum_val_int
    FROM scored
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY val_rank DESC) AS val_rank_rank,
        MIN(val_int_rank) OVER (PARTITION BY plan) AS plan_min_val_int_rank,
        ROUND(val_int_rank / SUM(val_int_rank) OVER (PARTITION BY plan), 4) AS val_int_rank_share,
        COUNT(val_int) OVER (PARTITION BY plan) AS plan_count_val_int
    FROM with_windows
),
enriched_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        plan,
        effective_from,
        val,
        clean_plan,
        val_int,
        val_band,
        val_int_percent_rank,
        plan_count_val,
        val_rank,
        val_int_rank,
        plan_stddev_samp_val_int,
        plan_sum_val_int,
        val_rank_rank,
        plan_min_val_int_rank,
        val_int_rank_share,
        plan_count_val_int,
        DENSE_RANK() OVER w_part AS val_int_rank_dense_rank,
        RANK() OVER (PARTITION BY plan ORDER BY val_rank_rank DESC NULLS FIRST) AS val_rank_rank_rank,
        ROUND(plan_min_val_int_rank / SUM(plan_min_val_int_rank) OVER (PARTITION BY plan), 4) AS plan_min_val_int_rank_share,
        DENSE_RANK() OVER (ORDER BY plan_count_val DESC NULLS FIRST) AS plan_count_val_dense_rank
    FROM enriched
    WINDOW w_part AS (ORDER BY val_int_rank ASC)
)
SELECT
    plan,
    effective_from,
    val,
    val_band,
    val_int_percent_rank,
    plan_count_val,
    val_int_rank,
    plan_sum_val_int,
    val_rank_rank,
    CASE WHEN plan RLIKE '^(pro|basic)$' THEN 'CORE' WHEN plan RLIKE '^(starter|enterprise)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS plan_zone
FROM enriched_price_tiers
ORDER BY val_int_rank DESC NULLS FIRST;
