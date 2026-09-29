WITH recent_moves AS (
    -- Filter stock_moves
    SELECT *
    FROM stock_moves
    WHERE (moved_at < TIMESTAMP '2025-03-02 09:00:00' OR unit_price BETWEEN 180.0 AND 307.8)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        MAX(unit_price) OVER (PARTITION BY YEAR(moved_at) ORDER BY moved_at DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS unit_price_max_4,
        COUNT(*) OVER (PARTITION BY operator, move_type) AS rows_in_operator_move_type
    FROM recent_moves
),
enriched AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.unit_price,
        s.unit_price_max_4,
        s.rows_in_operator_move_type,
        w.opened_on AS warehouse_opened_on,
        w.capacity AS warehouse_capacity,
        w.wh_code AS warehouse_wh_code,
        sk.weight_kg AS sku_weight_kg,
        sk.sku_code
    FROM with_trends s
    JOIN warehouses w ON w.id = s.warehouse_id AND w.city NOT IN ('Kuala Lumpur', 'Bangkok')
    JOIN skus sk ON sk.id = s.sku_id
    WHERE w.wh_code IS NOT NULL
),
with_activity AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
        )
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    unit_price,
    unit_price_max_4,
    rows_in_operator_move_type,
    warehouse_capacity,
    sku_weight_kg,
    ABS(warehouse_capacity - 4912) AS warehouse_capacity_distance,
    ABS(sku_weight_kg - 14.6) AS sku_weight_kg_distance
FROM with_activity
