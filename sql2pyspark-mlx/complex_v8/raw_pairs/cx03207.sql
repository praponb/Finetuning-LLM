WITH base_moves AS (
    -- Scope the stock_moves rows
    SELECT *
    FROM stock_moves
    WHERE HOUR(moved_at) BETWEEN 8 AND 13
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.unit_price,
        w.capacity AS warehouse_capacity,
        sk.unit_cost AS sku_unit_cost,
        sk.labels AS sku_labels
    FROM base_moves s
    JOIN warehouses w ON w.id = s.warehouse_id AND UPPER(TRIM(w.wh_code)) LIKE 'K%'
    LEFT JOIN skus sk ON sk.id = s.sku_id
    WHERE w.capacity <= 11506
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        warehouse_capacity,
        sku_unit_cost,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY move_type, operator), 2) AS unit_price_vs_move_type_operator_avg,
        ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY unit_price, id) AS rn,
        NTH_VALUE(sku_unit_cost, 2) OVER (PARTITION BY warehouse_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_sku_unit_cost,
        ROW_NUMBER() OVER (PARTITION BY sku_id, operator ORDER BY sku_unit_cost, id) AS rn_2,
        DENSE_RANK() OVER (PARTITION BY sku_id, operator ORDER BY sku_unit_cost, id) AS sku_unit_cost_dense_rank,
        MAX(qty) OVER (PARTITION BY sku_id, move_type) AS sku_id_move_type_max_qty,
        LEAD(sku_unit_cost) OVER (PARTITION BY move_type ORDER BY qty DESC, id) AS next_sku_unit_cost,
        RANK() OVER (PARTITION BY move_type ORDER BY qty DESC) AS qty_rank,
        sku_unit_cost - LAG(sku_unit_cost) OVER w_sku_id_operator_by_sku_unit AS sku_unit_cost_change
    FROM with_details
    WINDOW w_sku_id_operator_by_sku_unit AS (PARTITION BY sku_id, operator ORDER BY sku_unit_cost, id)
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(warehouse_capacity, 0) AS warehouse_capacity_filled,
        COALESCE(sku_unit_cost_change, 0.0) AS sku_unit_cost_change_filled,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
    FROM with_windows
)
-- Final projection
SELECT
    id,
    sku_id,
    move_type,
    operator,
    unit_price,
    warehouse_capacity,
    sku_unit_cost,
    unit_price_vs_move_type_operator_avg,
    second_sku_unit_cost,
    rn_2,
    sku_unit_cost_dense_rank,
    sku_id_move_type_max_qty,
    next_sku_unit_cost,
    sku_unit_cost_change,
    warehouse_capacity_filled,
    sku_unit_cost_change_filled,
    operator_slug,
    CASE WHEN move_type IN ('TRANSFER', 'ADJUST') THEN 'group_a' WHEN move_type = 'INBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group
FROM scored
