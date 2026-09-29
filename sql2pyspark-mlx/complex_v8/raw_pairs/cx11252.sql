WITH filtered_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at,
        SUBSTRING(TRIM(operator), 1, 4) AS operator_prefix
    FROM stock_moves
    WHERE warehouse_id >= 5
      AND unit_price >= 119.5
),
joined AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        s.unit_price,
        s.days_since_moved_at,
        s.operator_prefix,
        w.city AS warehouse_city
    FROM filtered_moves s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM joined
    WHERE days_since_moved_at > (SELECT AVG(days_since_moved_at) FROM joined)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(days_since_moved_at - 490) AS days_since_moved_at_distance,
        LOWER(TRIM(move_type)) AS move_type_clean
    FROM qualified
),
exploded_detail AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.unit_price,
        s.operator_prefix,
        s.days_since_moved_at_distance,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.warehouse_id AS move_warehouse_id,
        st.qty AS move_qty,
        st.moved_at AS move_moved_at
    FROM labeled s
    JOIN stock_moves st ON st.sku_id = s.sku_id
)
SELECT
    id,
    warehouse_id,
    unit_price,
    operator_prefix,
    move_id,
    move_unit_price,
    move_warehouse_id,
    move_qty,
    move_moved_at,
    DATE_FORMAT(move_moved_at, 'yyyy-MM-dd HH:00') AS move_moved_at_label,
    ROUND(unit_price * 1.5, 2) AS unit_price_adj
FROM exploded_detail
ORDER BY move_id, move_warehouse_id DESC
