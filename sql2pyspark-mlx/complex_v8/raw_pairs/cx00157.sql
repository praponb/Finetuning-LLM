WITH filtered_skus AS (
    -- Scope the skus rows
    SELECT
        id,
        category,
        unit_cost,
        weight_kg,
        is_active,
        ROUND(unit_cost * 1.07, 2) AS unit_cost_adj
    FROM skus
    WHERE category = 'Apparel'
      AND unit_cost >= 60.9
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        unit_cost,
        weight_kg,
        is_active,
        unit_cost_adj,
        LAST_VALUE(unit_cost) IGNORE NULLS OVER (ORDER BY unit_cost_adj DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_cost,
        SUM(unit_cost_adj) OVER (PARTITION BY category, is_active ORDER BY unit_cost_adj, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_cost_adj,
        LAST_VALUE(unit_cost) IGNORE NULLS OVER (PARTITION BY is_active, category ORDER BY weight_kg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_cost_2,
        DENSE_RANK() OVER w_category_by_unit_cost_adj_ti AS unit_cost_adj_dense_rank,
        FIRST_VALUE(unit_cost) IGNORE NULLS OVER (PARTITION BY is_active, category ORDER BY weight_kg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_cost,
        LEAD(weight_kg) OVER (PARTITION BY category, is_active ORDER BY unit_cost_adj, id) AS next_weight_kg,
        NTILE(5) OVER w_all_by_unit_cost AS unit_cost_quintile,
        unit_cost - LAG(unit_cost) OVER w_all_by_unit_cost_adj AS unit_cost_change,
        LEAD(weight_kg, 3) OVER (PARTITION BY category, is_active ORDER BY unit_cost_adj, id) AS next3_weight_kg,
        unit_cost_adj - LAG(unit_cost_adj) OVER (PARTITION BY is_active, category ORDER BY weight_kg, id) AS unit_cost_adj_change
    FROM filtered_skus
    WINDOW w_category_by_unit_cost_adj_ti AS (PARTITION BY category ORDER BY unit_cost_adj DESC NULLS FIRST), w_all_by_unit_cost AS (ORDER BY unit_cost ASC, id), w_all_by_unit_cost_adj AS (ORDER BY unit_cost_adj DESC NULLS FIRST, id)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        unit_cost,
        weight_kg,
        is_active,
        unit_cost_adj,
        last_known_unit_cost,
        running_unit_cost_adj,
        last_known_unit_cost_2,
        first_known_unit_cost,
        next_weight_kg,
        unit_cost_quintile,
        unit_cost_change,
        next3_weight_kg,
        unit_cost_adj_change
    FROM metrics
    WHERE unit_cost_adj_dense_rank = 1
      AND next3_weight_kg BETWEEN 3.7 AND 20.8
)
SELECT
    id,
    unit_cost,
    weight_kg,
    unit_cost_adj,
    last_known_unit_cost,
    running_unit_cost_adj,
    last_known_unit_cost_2,
    first_known_unit_cost,
    next_weight_kg,
    unit_cost_quintile,
    unit_cost_change,
    next3_weight_kg,
    unit_cost_adj_change,
    FLOOR(unit_cost_adj / 100) * 100 AS unit_cost_adj_bucket
FROM leaders
WHERE next_weight_kg <= 15.2
