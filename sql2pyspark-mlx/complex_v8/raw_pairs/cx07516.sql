WITH valid_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        DAYOFWEEK(moved_at) AS moved_at_dayofweek
    FROM stock_moves
    WHERE MONTH(moved_at) IN (1, 3)
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(st.qty), 2) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS avg_move_qty,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count_2
    FROM valid_moves s
),
enriched_warehouse AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator,
        s.unit_price,
        s.moved_at_dayofweek,
        s.avg_move_qty,
        w.city AS warehouse_city,
        w.opened_on AS warehouse_opened_on,
        w.capacity AS warehouse_capacity
    FROM with_counts s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
line_level_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.unit_price,
        s.avg_move_qty,
        s.warehouse_city,
        s.warehouse_capacity,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.unit_price AS move_unit_price,
        st.move_type,
        st.sku_id AS move_sku_id
    FROM enriched_warehouse s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
)
SELECT
    move_type,
    warehouse_id,
    SUM(CASE WHEN warehouse_city IN ('Kuala Lumpur', 'Singapore') THEN 1 ELSE 0 END) AS n_warehouse_city_in_kuala_lump,
    ARRAY_SORT(COLLECT_SET(warehouse_city)) AS warehouse_city_list
FROM line_level_moves
GROUP BY CUBE(move_type, warehouse_id)
ORDER BY move_type NULLS LAST, warehouse_id NULLS LAST
