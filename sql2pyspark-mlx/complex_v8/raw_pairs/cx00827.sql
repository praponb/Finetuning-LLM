WITH valid_moves AS (
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
        DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_label,
        CASE WHEN move_type IN ('ADJUST', 'TRANSFER') THEN 'group_a' WHEN move_type = 'INBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group
    FROM stock_moves
    WHERE qty BETWEEN 139 AND 299
      AND HOUR(moved_at) BETWEEN 7 AND 11
),
combined_details_warehouse AS (
    -- Join warehouses
    SELECT s.*, w.opened_on AS warehouse_opened_on, w.wh_code AS warehouse_wh_code
    FROM valid_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT SUM(st.unit_price) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS total_move_unit_price,
        (SELECT SUM(st.unit_price) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS total_move_unit_price_2,
        (SELECT COUNT(CASE WHEN st.operator IN ('op_chan', 'op_eve') THEN 1 END) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_operator_count
    FROM combined_details_warehouse s
)
SELECT
    move_type_group,
    move_operator_count,
    sku_id,
    COUNT(DISTINCT move_type_group) AS distinct_move_type_groups
FROM profiled
GROUP BY GROUPING SETS ((move_type_group, move_operator_count, sku_id), (move_type_group), (move_operator_count), ())
ORDER BY move_type_group NULLS LAST, move_operator_count NULLS LAST, sku_id NULLS LAST
