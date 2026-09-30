WITH filtered AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        CONCAT_WS(' / ', operator, move_type) AS operator_move_type_label,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price
    FROM stock_moves
),
enriched AS (
    -- Join warehouses
    SELECT
        s.*,
        w.capacity AS warehouse_capacity,
        w.wh_code AS warehouse_wh_code,
        sk.labels AS sku_labels
    FROM filtered s
    JOIN warehouses w ON w.id = s.warehouse_id
    JOIN skus sk ON sk.id = s.sku_id
    WHERE UPPER(TRIM(w.wh_code)) LIKE 'C%'
),
exploded_detail_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.unit_price,
        s.warehouse_capacity,
        s.warehouse_wh_code,
        st.id AS move_id,
        st.operator AS move_operator,
        st.qty AS move_qty,
        st.moved_at AS move_moved_at
    FROM enriched s
    JOIN stock_moves st ON st.sku_id = s.sku_id AND TO_DATE(st.moved_at) < DATE '2025-02-12'
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    unit_price,
    warehouse_capacity,
    move_operator,
    move_moved_at
FROM exploded_detail_moves
WHERE unit_price < 330.0
ORDER BY move_moved_at
