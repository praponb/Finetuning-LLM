WITH valid_skus AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        category,
        weight_kg,
        labels,
        is_active,
        GREATEST(weight_kg, unit_cost) AS greatest_weight_kg_unit_cost,
        LENGTH(TRIM(category)) AS category_len,
        ROUND(unit_cost * 1.07, 2) AS unit_cost_adj
    FROM skus
    WHERE weight_kg > 4.7
      AND unit_cost BETWEEN 60.9 AND 330.6
),
category_stats AS (
    -- Max unit_cost_adj per category
    SELECT category, MAX(unit_cost_adj) AS max_unit_cost_adj
    FROM valid_skus
    GROUP BY category
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_unit_cost_adj,
        ROUND(s.unit_cost_adj - g.max_unit_cost_adj, 2) AS unit_cost_adj_minus_max
    FROM valid_skus s
    JOIN category_stats g ON g.category = s.category
    WHERE s.unit_cost_adj = g.max_unit_cost_adj
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.id) AS last_moved_at,
        (SELECT COUNT(*) FILTER (WHERE st.operator IN ('op_ann', 'op_bee')) FROM stock_moves st WHERE st.sku_id = s.id) AS move_operator_count
    FROM outliers s
),
category_stats_2 AS (
    -- Max weight_kg per category
    SELECT category, MAX(weight_kg) AS max_weight_kg
    FROM with_counts
    GROUP BY category
),
outliers_2 AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_weight_kg,
        ROUND(s.weight_kg - g.max_weight_kg, 2) AS weight_kg_minus_max
    FROM with_counts s
    JOIN category_stats_2 g ON g.category = s.category
    WHERE s.weight_kg = g.max_weight_kg
)
SELECT
    is_active,
    category_len,
    MAX(last_moved_at) AS last_last_moved_at,
    ARRAY_SORT(COLLECT_SET(category)) AS category_list,
    ROUND(SUM(greatest_weight_kg_unit_cost) / NULLIF(SUM(unit_cost_adj), 0), 3) AS greatest_weight_kg_unit_cost_to_unit_cost_adj_ra
FROM outliers_2
GROUP BY GROUPING SETS ((is_active, category_len), (is_active), (category_len), ())
