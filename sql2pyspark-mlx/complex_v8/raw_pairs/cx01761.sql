WITH recent AS (
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
    WHERE warehouse_id <= 4
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        move_type,
        operator,
        unit_price,
        NTILE(4) OVER w_all_by_unit_price AS unit_price_quartile,
        MIN(unit_price) OVER (PARTITION BY operator, warehouse_id) AS operator_warehouse_id_min_unit_price
    FROM recent
    WINDOW w_all_by_unit_price AS (ORDER BY unit_price DESC, id)
),
detail_rows_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.move_type,
        s.unit_price,
        s.unit_price_quartile,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.qty AS move_qty,
        st.move_type AS move_move_type
    FROM with_trends s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.warehouse_id AND st.move_type NOT IN ('INBOUND', 'TRANSFER')
),
combined_details_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.move_type,
        s.unit_price_quartile,
        s.move_id,
        s.move_moved_at,
        s.move_qty,
        s.move_move_type,
        w.opened_on AS warehouse_opened_on,
        w.city AS warehouse_city
    FROM detail_rows_moves s
    JOIN warehouses w ON w.id = s.warehouse_id AND w.capacity > 4912
)
-- Final projection
SELECT
    id,
    move_type,
    unit_price_quartile,
    move_id,
    move_move_type,
    warehouse_opened_on,
    warehouse_city
FROM combined_details_warehouse
ORDER BY move_id NULLS LAST
