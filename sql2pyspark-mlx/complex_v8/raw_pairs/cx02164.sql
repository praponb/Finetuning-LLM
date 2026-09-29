WITH prepared AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        COALESCE(qty, 0) AS qty_filled,
        UPPER(TRIM(move_type)) AS move_type_clean
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_warehouse_id_operator_by_qty, 3) AS qty_filled_cume_dist,
        COUNT(*) OVER (PARTITION BY operator) AS rows_in_operator
    FROM prepared
    WINDOW w_warehouse_id_operator_by_qty AS (PARTITION BY warehouse_id, operator ORDER BY qty_filled ASC)
),
summary AS (
    -- Summarise per group
    SELECT
        TO_DATE(moved_at) AS moved_at_day,
        warehouse_id,
        MAX(moved_at) AS last_moved_at,
        SUM(CASE WHEN operator IN ('op_chan', 'op_eve') THEN 1 ELSE 0 END) AS n_operator_in_op_chan_op_eve,
        SUM(qty_filled_cume_dist) AS total_qty_filled_cume_dist,
        ROUND(AVG(qty_filled), 2) AS avg_qty_filled
    FROM ranked
    GROUP BY TO_DATE(moved_at), warehouse_id
),
joined AS (
    -- Join warehouses
    SELECT
        s.moved_at_day,
        s.warehouse_id,
        s.n_operator_in_op_chan_op_eve,
        s.total_qty_filled_cume_dist,
        s.avg_qty_filled,
        w.wh_code AS warehouse_wh_code,
        w.city AS warehouse_city
    FROM summary s
    JOIN warehouses w ON w.id = s.warehouse_id AND w.city NOT IN ('Singapore', 'Kuala Lumpur')
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT n_operator_in_op_chan_op_eve, moved_at_day, warehouse_city, total_qty_filled_cume_dist
    FROM joined
) src
PIVOT (
    MAX(total_qty_filled_cume_dist) AS max_v, COUNT(total_qty_filled_cume_dist) AS n
    FOR warehouse_city IN ('Bangkok', 'Singapore')
)
ORDER BY n_operator_in_op_chan_op_eve, moved_at_day
