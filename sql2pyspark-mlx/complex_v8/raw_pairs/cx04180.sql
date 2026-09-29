WITH base_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        CASE WHEN move_type IN ('OUTBOUND', 'INBOUND') THEN 'group_a' WHEN move_type = 'TRANSFER' THEN 'group_b' ELSE 'other' END AS move_type_group,
        unit_price + qty AS unit_price_plus_qty,
        unit_price - qty AS unit_price_minus_qty
    FROM stock_moves
    WHERE unit_price BETWEEN 111.7 AND 341.5
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS first_moved_at,
        (SELECT SUM(st.qty) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS total_move_qty,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_count
    FROM base_moves s
),
with_details_warehouse AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        s.unit_price,
        s.move_type_group,
        s.unit_price_plus_qty,
        s.unit_price_minus_qty,
        s.first_moved_at,
        s.total_move_qty,
        s.move_count,
        w.city AS warehouse_city
    FROM summarized_rows s
    JOIN warehouses w ON w.id = s.warehouse_id
),
labeled AS (
    -- Add calculated columns
    SELECT *, ABS(move_count - 5) AS move_count_distance
    FROM with_details_warehouse
    WHERE warehouse_id >= 5
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        unit_price,
        unit_price_plus_qty,
        unit_price_minus_qty,
        first_moved_at,
        total_move_qty,
        warehouse_city,
        SUM(move_count) OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_move_count,
        ROUND(AVG(unit_price) OVER (PARTITION BY move_type), 2) AS move_type_avg_unit_price,
        ROUND(CUME_DIST() OVER w_all_by_unit_price, 3) AS unit_price_cume_dist,
        ROUND(unit_price_minus_qty - AVG(unit_price_minus_qty) OVER (PARTITION BY move_type), 2) AS unit_price_minus_qty_vs_move_type_avg,
        DENSE_RANK() OVER w_all_by_unit_price AS unit_price_dense_rank
    FROM labeled
    WINDOW w_all_by_unit_price AS (ORDER BY unit_price, id)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        warehouse_city,
        DATE_FORMAT(first_moved_at, 'yyyy-MM') AS first_moved_at_ym,
        ROUND(STDDEV(unit_price_minus_qty_vs_move_type_avg), 2) AS stddev_unit_price_minus_qty_vs_move_type_avg,
        ARRAY_SORT(COLLECT_SET(warehouse_city)) AS warehouse_city_list,
        MAX(unit_price_cume_dist) AS max_unit_price_cume_dist
    FROM metrics
    GROUP BY warehouse_city, DATE_FORMAT(first_moved_at, 'yyyy-MM')
)
-- Final projection
SELECT warehouse_city, max_unit_price_cume_dist
FROM rollup_base
ORDER BY max_unit_price_cume_dist, warehouse_city NULLS LAST
