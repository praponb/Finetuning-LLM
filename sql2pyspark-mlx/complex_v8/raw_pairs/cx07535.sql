WITH recent_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        ABS(unit_price - 289.7) AS unit_price_distance
    FROM stock_moves
    WHERE warehouse_id <= 4
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        COUNT(CASE WHEN move_type IN ('TRANSFER', 'INBOUND', 'OUTBOUND') THEN 1 END) AS move_move_type_count
    FROM stock_moves
    GROUP BY warehouse_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_move_type_count, 0) AS move_move_type_count
    FROM recent_moves s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
),
combined_details AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price_distance,
        s.move_move_type_count,
        w.capacity AS warehouse_capacity,
        w.opened_on AS warehouse_opened_on,
        w.city AS warehouse_city,
        sk.unit_cost AS sku_unit_cost
    FROM with_stats s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        move_type,
        operator,
        unit_price_distance,
        move_move_type_count,
        warehouse_capacity,
        warehouse_opened_on,
        warehouse_city,
        sku_unit_cost,
        TO_DATE(moved_at) AS moved_at_date,
        CONCAT_WS('-', operator, warehouse_city) AS operator_warehouse_city_label,
        DATEDIFF(DATE '2026-01-01', warehouse_opened_on) AS days_since_warehouse_opened_on
    FROM combined_details
),
labeled AS (
    -- Add calculated columns
    SELECT *, DATE_ADD(warehouse_opened_on, 30) AS warehouse_opened_on_plus_30d
    FROM tagged
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM labeled
    WHERE sku_unit_cost > (SELECT AVG(sku_unit_cost) FROM labeled)
)
SELECT
    id,
    operator,
    unit_price_distance,
    move_move_type_count,
    warehouse_opened_on,
    sku_unit_cost,
    operator_warehouse_city_label,
    days_since_warehouse_opened_on,
    warehouse_opened_on_plus_30d
FROM screened
ORDER BY unit_price_distance, id
