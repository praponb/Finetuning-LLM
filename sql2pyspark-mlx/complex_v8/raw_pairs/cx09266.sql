WITH base_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        CONCAT(operator, '#', CAST(warehouse_id AS STRING)) AS operator_ref,
        SUBSTRING(TRIM(move_type), 1, 4) AS move_type_prefix
    FROM stock_moves
    WHERE qty > 116
),
combined_details AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.move_type_prefix,
        sk.category AS sku_category,
        sk.weight_kg AS sku_weight_kg,
        sk.is_active AS sku_is_active
    FROM base_moves s
    JOIN skus sk ON sk.id = s.sku_id
    WHERE sk.category = 'Toys'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        move_type_prefix,
        sku_category,
        sku_weight_kg,
        sku_is_active,
        ROW_NUMBER() OVER (PARTITION BY sku_is_active, move_type ORDER BY sku_weight_kg DESC, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY operator, warehouse_id ORDER BY moved_at ASC, id) AS seq_no_2,
        NTH_VALUE(sku_weight_kg, 2) OVER (PARTITION BY sku_is_active, move_type ORDER BY sku_weight_kg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_sku_weight_kg,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER (PARTITION BY sku_is_active, move_type ORDER BY sku_weight_kg DESC, id))) / 60, 1) AS minutes_since_prev_moved_at,
        FIRST_VALUE(moved_at) OVER (PARTITION BY sku_is_active, move_type ORDER BY sku_weight_kg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_moved_at,
        SUM(sku_weight_kg) OVER (PARTITION BY sku_is_active, move_type ORDER BY sku_weight_kg DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS sku_weight_kg_sum_1_prior,
        sku_weight_kg - LAG(sku_weight_kg) OVER w_operator_by_moved_at AS sku_weight_kg_change,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY sku_is_active, move_type ORDER BY sku_weight_kg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        MIN(sku_weight_kg) OVER w_move_type AS move_type_min_sku_weight_kg,
        ROUND(sku_weight_kg / NULLIF(SUM(sku_weight_kg) OVER w_sku_id_move_type, 0), 4) AS sku_weight_kg_share_of_sku_id_move_type
    FROM combined_details
    WINDOW w_operator_by_moved_at AS (PARTITION BY operator ORDER BY moved_at, id), w_move_type AS (PARTITION BY move_type), w_sku_id_move_type AS (PARTITION BY sku_id, move_type)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        move_type_prefix,
        sku_category,
        sku_weight_kg,
        sku_is_active,
        seq_no,
        second_sku_weight_kg,
        minutes_since_prev_moved_at,
        first_moved_at,
        sku_weight_kg_sum_1_prior,
        sku_weight_kg_change,
        second_moved_at,
        move_type_min_sku_weight_kg,
        sku_weight_kg_share_of_sku_id_move_type
    FROM with_windows
    WHERE seq_no_2 = 1
      AND warehouse_id <= 4
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        sku_id,
        SUM(sku_weight_kg_change) AS total_sku_weight_kg_change,
        COUNT(*) FILTER (WHERE sku_category IN ('Toys', 'Food', 'Beauty')) AS n_sku_category_in_toys_food_be,
        COUNT(*) AS cnt,
        COUNT(DISTINCT operator) AS distinct_operators,
        COUNT(DISTINCT sku_id) AS distinct_sku_ids,
        ROUND(SUM(move_type_min_sku_weight_kg), 2) AS total_move_type_min_sku_weight_kg
    FROM top_per_group
    GROUP BY sku_id
)
-- Final projection
SELECT
    sku_id,
    total_sku_weight_kg_change,
    n_sku_category_in_toys_food_be,
    distinct_operators,
    distinct_sku_ids,
    total_move_type_min_sku_weight_kg
FROM rollup_base
ORDER BY total_move_type_min_sku_weight_kg, distinct_operators DESC, sku_id
LIMIT 20
