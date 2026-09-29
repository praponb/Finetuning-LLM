WITH base AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE operator = 'op_bee'
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(unit_price * 0.25, 2) AS unit_price_adj,
        CASE WHEN unit_price >= 299.1 THEN 'A' WHEN unit_price >= 130.6 THEN 'B' ELSE 'C' END AS unit_price_tier,
        DATE_FORMAT(moved_at, 'MMM yyyy') AS moved_at_label
    FROM base
    WHERE sku_id <= 9
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN st.operator = 'op_dew' THEN 1 END) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_operator_count,
        (SELECT SUM(st.qty) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS total_move_qty
    FROM derived s
),
exploded_detail AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.unit_price_tier,
        s.move_operator_count,
        s.total_move_qty,
        st.id AS move_id,
        st.warehouse_id AS move_warehouse_id,
        st.moved_at AS move_moved_at,
        st.unit_price AS move_unit_price,
        st.move_type AS move_move_type
    FROM profiled s
    JOIN stock_moves st ON st.sku_id = s.sku_id AND TO_DATE(st.moved_at) < DATE '2025-02-24'
),
detail_rows AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.unit_price_tier,
        s.move_operator_count,
        s.total_move_qty,
        s.move_id,
        s.move_warehouse_id,
        s.move_moved_at,
        s.move_move_type,
        st.id AS move_id_2,
        st.warehouse_id AS move_warehouse_id_2,
        st.move_type AS move_move_type_2,
        st.unit_price AS move_unit_price_2
    FROM exploded_detail s
    JOIN stock_moves st ON st.sku_id = s.sku_id AND st.unit_price > 73.0
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN st.operator <> 'op_bee' THEN 1 END) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_operator_count_2
    FROM detail_rows s
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.unit_price_tier,
        s.move_operator_count,
        s.total_move_qty,
        s.move_id,
        s.move_warehouse_id,
        s.move_move_type,
        s.move_id_2,
        s.move_warehouse_id_2,
        s.move_move_type_2,
        s.move_unit_price_2,
        w.opened_on AS warehouse_opened_on,
        w.capacity AS warehouse_capacity,
        st.operator AS move_operator,
        st.move_type
    FROM summarized_rows s
    JOIN warehouses w ON w.id = s.move_warehouse_id_2
    LEFT JOIN stock_moves st ON st.id = s.move_id_2
)
SELECT
    move_warehouse_id,
    warehouse_id,
    MIN(warehouse_capacity) AS min_warehouse_capacity,
    ROUND(SUM(warehouse_capacity) / NULLIF(SUM(qty), 0), 3) AS warehouse_capacity_to_qty_ratio
FROM combined_details
GROUP BY ROLLUP(move_warehouse_id, warehouse_id)
ORDER BY move_warehouse_id NULLS LAST, warehouse_id NULLS LAST
