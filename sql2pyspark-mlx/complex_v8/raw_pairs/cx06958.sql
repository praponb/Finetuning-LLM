WITH scoped_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        LENGTH(TRIM(move_type)) AS move_type_len
    FROM stock_moves
    WHERE move_type IS NOT NULL
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE st.move_type IN ('OUTBOUND', 'INBOUND')) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_move_type_count,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS last_moved_at
    FROM scoped_moves s
),
with_details_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.unit_price,
        s.move_move_type_count,
        s.last_moved_at,
        w.capacity AS warehouse_capacity,
        w.wh_code AS warehouse_wh_code,
        w.city AS warehouse_city
    FROM summarized_rows s
    JOIN warehouses w ON w.id = s.warehouse_id
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        warehouse_id,
        warehouse_city,
        MIN(unit_price) AS min_unit_price,
        ARRAY_SORT(COLLECT_SET(warehouse_city)) AS warehouse_city_list
    FROM with_details_warehouse
    GROUP BY warehouse_id, warehouse_city
),
without_matches AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM grouped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.sku_id >= 8
        )
)
SELECT warehouse_id, min_unit_price
FROM without_matches
ORDER BY min_unit_price DESC, warehouse_id DESC NULLS FIRST
