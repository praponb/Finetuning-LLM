WITH valid_moves AS (
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
),
joined_sku AS (
    -- Attach sku attributes
    SELECT
        s.*,
        sk.labels AS sku_labels,
        sk.is_active AS sku_is_active,
        sk.category AS sku_category
    FROM valid_moves s
    JOIN skus sk ON sk.id = s.sku_id
),
joined_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.sku_labels,
        s.sku_is_active,
        s.sku_category,
        w.opened_on AS warehouse_opened_on,
        sk.labels AS sku_labels_2
    FROM joined_sku s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
    WHERE w.opened_on < DATE '2025-07-03'
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        COUNT(CASE WHEN move_type IN ('INBOUND', 'ADJUST', 'TRANSFER') THEN 1 END) AS move_move_type_count
    FROM stock_moves
    GROUP BY sku_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_move_type_count, 0) AS move_move_type_count
    FROM joined_warehouse s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
with_parent AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        s.unit_price,
        s.sku_labels,
        s.sku_is_active,
        s.warehouse_opened_on,
        s.sku_labels_2,
        s.move_move_type_count,
        w.city AS warehouse_city
    FROM with_stats s
    JOIN warehouses w ON w.id = s.warehouse_id AND w.capacity < 11506
),
summary AS (
    -- Summarise per group
    SELECT
        warehouse_id,
        ROUND(AVG(unit_price), 2) AS avg_unit_price,
        SUM(CASE WHEN move_type = 'INBOUND' THEN 1 ELSE 0 END) AS n_move_type_inbound
    FROM with_parent
    GROUP BY 1
)
SELECT warehouse_id, avg_unit_price, n_move_type_inbound
FROM summary
WHERE avg_unit_price <= 350.3
