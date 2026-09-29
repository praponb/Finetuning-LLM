WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        ROUND(weight_kg * 0.9, 2) AS weight_kg_adj
    FROM skus
    WHERE unit_cost >= 108.5
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY is_active, category ORDER BY unit_cost, id) AS seq_no,
        LAST_VALUE(weight_kg) IGNORE NULLS OVER (PARTITION BY category ORDER BY weight_kg_adj, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_weight_kg,
        LAST_VALUE(unit_cost) IGNORE NULLS OVER (PARTITION BY category, is_active ORDER BY weight_kg ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_cost,
        ROUND(weight_kg - AVG(weight_kg) OVER w_is_active_category, 2) AS weight_kg_vs_is_active_category_avg,
        NTH_VALUE(unit_cost, 2) OVER (PARTITION BY category ORDER BY weight_kg_adj, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_cost,
        RANK() OVER (PARTITION BY category, is_active ORDER BY weight_kg ASC) AS weight_kg_rank,
        DENSE_RANK() OVER (ORDER BY unit_cost ASC NULLS LAST) AS unit_cost_dense_rank,
        NTH_VALUE(unit_cost, 2) OVER (ORDER BY unit_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_cost_2,
        MAX(unit_cost) OVER (PARTITION BY is_active, category ORDER BY weight_kg_adj, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS unit_cost_max_1_prior,
        MAX(weight_kg) OVER (PARTITION BY category) AS category_max_weight_kg,
        ROUND(AVG(unit_cost) OVER (PARTITION BY is_active, category ORDER BY weight_kg_adj, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS unit_cost_avg_6_prior
    FROM scoped
    WINDOW w_is_active_category AS (PARTITION BY is_active, category)
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM windowed
    WHERE seq_no <= 5
      AND unit_cost >= 68.0
)
SELECT
    id,
    category,
    unit_cost,
    weight_kg,
    labels,
    weight_kg_adj,
    seq_no,
    last_known_weight_kg,
    last_known_unit_cost,
    weight_kg_vs_is_active_category_avg,
    weight_kg_rank,
    unit_cost_dense_rank,
    second_unit_cost_2,
    category_max_weight_kg
FROM first_rows
ORDER BY weight_kg_adj DESC, last_known_unit_cost DESC
