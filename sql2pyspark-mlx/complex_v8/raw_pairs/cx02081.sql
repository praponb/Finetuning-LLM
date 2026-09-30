WITH recent AS (
    -- Scope the skus rows
    SELECT
        id,
        category,
        weight_kg,
        labels,
        is_active,
        FLOOR(unit_cost / 50) * 50 AS unit_cost_bucket
    FROM skus
    WHERE (sku_code IS NOT NULL OR weight_kg BETWEEN 1.8 AND 20.8)
),
summary AS (
    -- Group-level totals
    SELECT
        is_active,
        ROUND(STDDEV(unit_cost_bucket), 2) AS stddev_unit_cost_bucket,
        ROUND(100 * COUNT(CASE WHEN is_active = TRUE THEN 1 END) / COUNT(*), 1) AS pct_is_active_true
    FROM recent
    GROUP BY 1
    HAVING MAX(weight_kg) > 4.7
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM summary
    WHERE stddev_unit_cost_bucket > (SELECT AVG(stddev_unit_cost_bucket) FROM summary)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        is_active,
        stddev_unit_cost_bucket,
        MIN(pct_is_active_true) OVER (PARTITION BY is_active ORDER BY stddev_unit_cost_bucket DESC, is_active ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_pct_is_active_true_so_far,
        LEAD(pct_is_active_true, 1, 0) OVER (PARTITION BY is_active ORDER BY pct_is_active_true, is_active) AS next_pct_is_active_true,
        SUM(stddev_unit_cost_bucket) OVER (PARTITION BY is_active ORDER BY pct_is_active_true, is_active ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS stddev_unit_cost_bucket_sum_3,
        ROUND(pct_is_active_true / NULLIF(SUM(pct_is_active_true) OVER w_is_active, 0), 4) AS pct_is_active_true_share_of_is_active
    FROM without_matches
    WINDOW w_is_active AS (PARTITION BY is_active)
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM ranked
    WHERE pct_is_active_true_share_of_is_active > (SELECT MIN(pct_is_active_true_share_of_is_active) FROM ranked)
),
with_activity_2 AS (
    -- Compare against the overall max
    SELECT *
    FROM with_activity
    WHERE next_pct_is_active_true < (SELECT MAX(next_pct_is_active_true) FROM with_activity)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        is_active,
        min_pct_is_active_true_so_far,
        next_pct_is_active_true,
        stddev_unit_cost_bucket_sum_3,
        SUM(next_pct_is_active_true) OVER (ORDER BY stddev_unit_cost_bucket DESC, is_active ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS next_pct_is_active_true_sum_1_prior,
        ROUND(AVG(next_pct_is_active_true) OVER (ORDER BY stddev_unit_cost_bucket DESC, is_active ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS next_pct_is_active_true_avg_2,
        ROUND(next_pct_is_active_true - AVG(next_pct_is_active_true) OVER (PARTITION BY is_active), 2) AS next_pct_is_active_true_vs_is_active_avg,
        ROW_NUMBER() OVER w_all_by_stddev_unit_cost_buck_3 AS seq_no,
        LAG(stddev_unit_cost_bucket) OVER w_all_by_stddev_unit_cost_buck_3 AS prev_stddev_unit_cost_bucket,
        LAG(next_pct_is_active_true, 3, 0) OVER w_all_by_stddev_unit_cost_buck_3 AS prev3_next_pct_is_active_true
    FROM with_activity_2
    WINDOW w_all_by_stddev_unit_cost_buck_3 AS (ORDER BY stddev_unit_cost_bucket DESC, is_active)
)
SELECT
    is_active,
    min_pct_is_active_true_so_far,
    next_pct_is_active_true_sum_1_prior,
    next_pct_is_active_true_vs_is_active_avg,
    seq_no,
    prev_stddev_unit_cost_bucket,
    prev3_next_pct_is_active_true
FROM with_trends
ORDER BY seq_no, is_active
LIMIT 10
