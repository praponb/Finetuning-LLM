WITH scoped_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown,
        LEAST(unit_price, qty) AS least_unit_price_qty,
        CONCAT_WS(' / ', operator, move_type) AS operator_move_type_label
    FROM stock_moves
    WHERE unit_price >= 60.4
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count
    FROM scoped_moves s
),
joined_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.*,
        w.capacity AS warehouse_capacity,
        sk.weight_kg AS sku_weight_kg,
        sk.unit_cost AS sku_unit_cost
    FROM with_counts s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id AND w.city = 'Singapore'
    JOIN skus sk ON sk.id = s.sku_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(move_count, 2) OVER (ORDER BY move_count DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_move_count,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY operator, operator_or_unknown)) AS distinct_warehouse_id_in_operator_operator_or_un,
        FIRST_VALUE(sku_weight_kg) OVER (ORDER BY move_count DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_sku_weight_kg
    FROM joined_warehouse
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        first_sku_weight_kg - LAG(first_sku_weight_kg) OVER (ORDER BY distinct_warehouse_id_in_operator_operator_or_un, id) AS first_sku_weight_kg_change,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY move_count, second_move_count), 0), 4) AS qty_share_of_move_count_second_move_count,
        COUNT(*) OVER (PARTITION BY operator_or_unknown ORDER BY least_unit_price_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM with_windows
)
-- Final projection
SELECT
    id,
    warehouse_id,
    qty,
    operator,
    unit_price,
    operator_or_unknown,
    least_unit_price_qty,
    operator_move_type_label,
    warehouse_capacity,
    sku_weight_kg,
    distinct_warehouse_id_in_operator_operator_or_un,
    first_sku_weight_kg,
    first_sku_weight_kg_change,
    running_count
FROM ranked
WHERE distinct_warehouse_id_in_operator_operator_or_un > 3
