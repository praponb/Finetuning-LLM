WITH cleaned_moves AS (
    -- Scope the stock_moves rows
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
    WHERE sku_id IS NOT NULL
      AND unit_price >= 209.4
),
with_details_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        s.unit_price,
        w.opened_on AS warehouse_opened_on,
        w.wh_code AS warehouse_wh_code
    FROM cleaned_moves s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT sku_id, MAX(moved_at) AS last_moved_at
    FROM stock_moves
    GROUP BY sku_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.last_moved_at
    FROM with_details_warehouse s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.warehouse_opened_on,
        s.warehouse_wh_code,
        s.last_moved_at,
        w.capacity AS warehouse_capacity,
        w.city AS warehouse_city,
        sk.weight_kg AS sku_weight_kg
    FROM with_counts s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
    WHERE w.city = 'Singapore'
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.unit_price,
        s.warehouse_wh_code,
        s.last_moved_at,
        st.id AS move_id,
        st.move_type,
        st.qty AS move_qty,
        st.moved_at AS move_moved_at
    FROM with_parent s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id AND st.move_type = 'INBOUND'
),
enriched_move AS (
    -- Attach move attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.warehouse_wh_code,
        s.last_moved_at,
        s.move_id,
        s.move_type,
        s.move_qty,
        s.move_moved_at,
        st.operator AS move_operator
    FROM with_children_moves s
    LEFT JOIN stock_moves st ON st.id = s.move_id
)
SELECT
    id,
    warehouse_id,
    sku_id,
    moved_at,
    warehouse_wh_code,
    last_moved_at,
    move_type,
    move_qty,
    move_operator
FROM enriched_move
WHERE move_qty >= 198
