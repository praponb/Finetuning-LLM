WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE_OUTER(tier_limits) AS val
    FROM price_tiers
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        DENSE_RANK() OVER w_ordered val_dense_rank,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY val DESC NULLS LAST) val_dense_rank_2,
        AVG(val) OVER (PARTITION BY plan) plan_avg_val,
        MAX(val) OVER (PARTITION BY MONTH(effective_from), plan) effective_from_month_max_val,
        PERCENT_RANK() OVER (PARTITION BY MONTH(effective_from), plan ORDER BY val DESC) val_percent_rank
    FROM exploded
    WINDOW w_ordered AS (ORDER BY val DESC)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY effective_from_month_max_val DESC NULLS FIRST) effective_from_month_max_val_percent_rank,
        DENSE_RANK() OVER (ORDER BY val_dense_rank_2 NULLS LAST) val_dense_rank_2_dense_rank
    FROM with_windows
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(val_percent_rank) OVER w_hist plan_count_val_percent_rank,
        AVG(effective_from_month_max_val) OVER w_hist plan_avg_effective_from_month_max_val,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY effective_from_month_max_val_percent_rank NULLS FIRST) effective_from_month_max_val_percent_rank_cume_dist
    FROM enriched
    WINDOW w_hist AS (PARTITION BY plan)
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, plan
    FROM metrics
    EXCEPT ALL
    SELECT id, plan
    FROM price_tiers
    WHERE effective_from < '2023-08-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN kept_keys k ON s.id = k.id AND s.plan = k.plan
)
-- Aggregate with conditional metrics
SELECT
    plan,
    COUNT(*) FILTER (WHERE plan = 'enterprise') AS enterprise_rows,
    COUNT(DISTINCT plan) AS distinct_plans,
    ROUND(AVG(val_dense_rank_2_dense_rank), 2) AS avg_val_dense_rank_2_dense_rank,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    COUNT(*) AS cnt,
    ROUND(SUM(plan_count_val_percent_rank) / NULLIF(COUNT(CASE WHEN plan = 'pro' THEN 1 END), 0), 2) AS plan_count_val_percent_rank_per_pro
FROM verified
GROUP BY ROLLUP (plan)
ORDER BY avg_val_dense_rank_2_dense_rank DESC;
