WITH scoped_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        FLOOR(qty / 100) * 100 AS qty_bucket,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price
    FROM stock_moves
),
enriched_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.qty_per_unit_price,
        w.city AS warehouse_city,
        sk.category AS sku_category
    FROM scoped_moves s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
detail_rows AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.qty_per_unit_price,
        s.sku_category,
        st.id AS move_id,
        st.warehouse_id AS move_warehouse_id,
        st.move_type AS move_move_type,
        st.moved_at AS move_moved_at
    FROM enriched_warehouse s
    JOIN stock_moves st ON st.sku_id = s.sku_id
),
combined_details_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.qty_per_unit_price,
        s.sku_category,
        s.move_id,
        s.move_warehouse_id,
        s.move_move_type,
        s.move_moved_at,
        w.city AS warehouse_city
    FROM detail_rows s
    JOIN warehouses w ON w.id = s.move_warehouse_id
)
-- Subtotals with GROUPING SETS
SELECT
    move_move_type,
    warehouse_city,
    move_warehouse_id,
    COUNT(DISTINCT move_move_type) AS distinct_move_move_types,
    ROUND(SUM(qty) / NULLIF(SUM(qty_per_unit_price), 0), 3) AS qty_to_qty_per_unit_price_ratio,
    MAX(move_moved_at) AS last_move_moved_at
FROM combined_details_warehouse
GROUP BY GROUPING SETS ((move_move_type, warehouse_city, move_warehouse_id), (move_move_type))
ORDER BY move_move_type NULLS LAST, warehouse_city NULLS LAST, move_warehouse_id NULLS LAST
