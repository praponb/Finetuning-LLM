WITH valid_skus AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        FLOOR(unit_cost / 50) * 50 AS unit_cost_bucket,
        CONCAT(category, '#', CAST(id AS STRING)) AS category_ref
    FROM skus
    WHERE NOT ARRAY_CONTAINS(labels, 'fragile')
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        sku_code,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        ROUND(AVG(unit_cost) OVER (PARTITION BY is_active, category ORDER BY unit_cost, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS unit_cost_avg_4,
        ROUND(CUME_DIST() OVER (PARTITION BY category, is_active ORDER BY weight_kg), 3) AS weight_kg_cume_dist,
        NTILE(4) OVER (PARTITION BY category, is_active ORDER BY weight_kg, id) AS weight_kg_quartile,
        SUM(unit_cost_bucket) OVER (PARTITION BY category, is_active ORDER BY weight_kg, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS unit_cost_bucket_sum_4,
        LEAD(weight_kg, 3) OVER (PARTITION BY is_active ORDER BY unit_cost ASC, id) AS next3_weight_kg,
        LAG(unit_cost_bucket) OVER (PARTITION BY is_active ORDER BY unit_cost ASC, id) AS prev_unit_cost_bucket,
        RANK() OVER (PARTITION BY is_active ORDER BY unit_cost ASC) AS unit_cost_rank,
        DENSE_RANK() OVER w_is_active_by_unit_cost_ties AS unit_cost_dense_rank,
        MAX(unit_cost) OVER (PARTITION BY category, is_active ORDER BY weight_kg, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS unit_cost_max_3_prior
    FROM valid_skus
    WINDOW w_is_active_by_unit_cost_ties AS (PARTITION BY is_active ORDER BY unit_cost ASC)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(prev_unit_cost_bucket) OVER (PARTITION BY unit_cost_dense_rank ORDER BY prev_unit_cost_bucket ASC, id) AS next_prev_unit_cost_bucket,
        ROUND(AVG(unit_cost_max_3_prior) OVER (PARTITION BY unit_cost_dense_rank ORDER BY prev_unit_cost_bucket ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS unit_cost_max_3_prior_avg_3_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY unit_cost_dense_rank ORDER BY unit_cost_bucket_sum_4 DESC), 3) AS unit_cost_bucket_sum_4_cume_dist,
        LAG(unit_cost_rank) OVER (PARTITION BY unit_cost_dense_rank ORDER BY prev_unit_cost_bucket ASC, id) AS prev_unit_cost_rank
    FROM sequenced
)
-- Final projection
SELECT DISTINCT next_prev_unit_cost_bucket
FROM metrics
ORDER BY next_prev_unit_cost_bucket DESC
