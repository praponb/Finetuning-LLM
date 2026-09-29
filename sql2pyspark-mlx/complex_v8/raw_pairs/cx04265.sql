WITH base_skus AS (
    -- Filter skus
    SELECT *
    FROM skus
    WHERE is_active = TRUE
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        category,
        weight_kg,
        labels,
        NTILE(4) OVER w_category_by_weight_kg AS weight_kg_quartile,
        LAG(unit_cost) OVER w_category_is_active_by_unit_c AS prev_unit_cost,
        ROUND(CUME_DIST() OVER (PARTITION BY category ORDER BY weight_kg), 3) AS weight_kg_cume_dist,
        COUNT(*) OVER (PARTITION BY is_active) AS rows_in_is_active,
        DENSE_RANK() OVER (PARTITION BY category, is_active ORDER BY unit_cost NULLS LAST) AS unit_cost_dense_rank,
        ROUND(AVG(weight_kg) OVER (PARTITION BY is_active, category), 2) AS is_active_category_avg_weight_kg,
        ROUND(unit_cost / NULLIF(SUM(unit_cost) OVER (PARTITION BY is_active, category), 0), 4) AS unit_cost_share_of_is_active_category,
        FIRST_VALUE(unit_cost) IGNORE NULLS OVER (PARTITION BY category, is_active ORDER BY unit_cost NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_cost,
        NTILE(3) OVER w_all_by_weight_kg AS weight_kg_tercile,
        NTH_VALUE(unit_cost, 2) OVER (PARTITION BY category, is_active ORDER BY unit_cost NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_cost,
        LAST_VALUE(unit_cost) IGNORE NULLS OVER (PARTITION BY category, is_active ORDER BY unit_cost NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_cost
    FROM base_skus
    WINDOW w_category_by_weight_kg AS (PARTITION BY category ORDER BY weight_kg, id), w_category_is_active_by_unit_c AS (PARTITION BY category, is_active ORDER BY unit_cost NULLS LAST, id), w_all_by_weight_kg AS (ORDER BY weight_kg DESC, id)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY category, weight_kg_quartile ORDER BY is_active_category_avg_weight_kg DESC, id) AS rn,
        DENSE_RANK() OVER (ORDER BY rows_in_is_active) AS rows_in_is_active_dense_rank,
        ROUND(AVG(weight_kg_cume_dist) OVER (PARTITION BY category, weight_kg_quartile ORDER BY is_active_category_avg_weight_kg DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS weight_kg_cume_dist_avg_4,
        FIRST_VALUE(weight_kg_tercile) OVER (PARTITION BY category, weight_kg_quartile ORDER BY is_active_category_avg_weight_kg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_weight_kg_tercile,
        LEAD(unit_cost_dense_rank, 2) OVER (ORDER BY rows_in_is_active, id) AS next2_unit_cost_dense_rank,
        SUM(weight_kg_quartile) OVER (PARTITION BY weight_kg_tercile, unit_cost_dense_rank ORDER BY weight_kg_cume_dist ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS weight_kg_quartile_sum_4,
        FIRST_VALUE(prev_unit_cost) OVER (PARTITION BY category, weight_kg_quartile ORDER BY is_active_category_avg_weight_kg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_prev_unit_cost
    FROM windowed
),
summary AS (
    -- Summarise per group
    SELECT
        rn,
        PERCENTILE_APPROX(unit_cost_dense_rank, 0.5) AS median_unit_cost_dense_rank,
        ROUND(100 * COUNT(CASE WHEN category NOT IN ('Food', 'Beauty') THEN 1 END) / COUNT(*), 1) AS pct_category_not_in_food_beauty,
        ARRAY_SORT(COLLECT_SET(category)) AS category_list
    FROM ranked
    GROUP BY rn
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER w_median_unit_cost_dense_rank AS median_unit_cost_dense_rank_rank,
        NTILE(3) OVER (PARTITION BY median_unit_cost_dense_rank, rn ORDER BY median_unit_cost_dense_rank ASC, rn) AS median_unit_cost_dense_rank_tercile,
        median_unit_cost_dense_rank - LAG(median_unit_cost_dense_rank) OVER (PARTITION BY median_unit_cost_dense_rank, rn ORDER BY median_unit_cost_dense_rank ASC, rn) AS median_unit_cost_dense_rank_change,
        COUNT(*) OVER w_median_unit_cost_dense_rank_2 AS rows_in_median_unit_cost_dense_rank
    FROM summary
    WINDOW w_median_unit_cost_dense_rank AS (PARTITION BY median_unit_cost_dense_rank, rn ORDER BY median_unit_cost_dense_rank ASC, rn), w_median_unit_cost_dense_rank_2 AS (PARTITION BY median_unit_cost_dense_rank)
)
SELECT
    rn,
    median_unit_cost_dense_rank,
    pct_category_not_in_food_beauty,
    category_list,
    median_unit_cost_dense_rank_tercile,
    median_unit_cost_dense_rank_change,
    rows_in_median_unit_cost_dense_rank,
    FLOOR(rn / 2) * 2 AS rn_bucket
FROM with_trends
WHERE median_unit_cost_dense_rank_tercile < 7
ORDER BY pct_category_not_in_food_beauty DESC, median_unit_cost_dense_rank_tercile NULLS LAST
