WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        ABS(qty - 271) AS qty_distance,
        ABS(unit_price - 277.6) AS unit_price_distance
    FROM stock_moves
    WHERE qty >= 191
),
move_type_stats AS (
    -- Avg qty_distance per move_type
    SELECT move_type, AVG(qty_distance) AS avg_qty_distance
    FROM scoped
    GROUP BY move_type
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_qty_distance,
        ROUND(s.qty_distance - g.avg_qty_distance, 2) AS qty_distance_minus_avg
    FROM scoped s
    JOIN move_type_stats g ON g.move_type = s.move_type
    WHERE s.qty_distance > g.avg_qty_distance
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.unit_price_distance,
        s.avg_qty_distance,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.warehouse_id AS move_warehouse_id
    FROM outliers s
    JOIN stock_moves st ON st.sku_id = s.sku_id
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    moved_at,
    avg_qty_distance,
    move_unit_price,
    move_warehouse_id,
    DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at,
    CASE WHEN avg_qty_distance >= 375.0 THEN 'HIGH' WHEN avg_qty_distance >= 200.0 THEN 'MEDIUM' ELSE 'LOW' END AS avg_qty_distance_tier
FROM with_children
