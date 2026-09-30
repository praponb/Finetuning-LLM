WITH scoped_moves AS (
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
        COALESCE(qty, 0) AS qty_filled
    FROM stock_moves
    WHERE move_type = 'TRANSFER'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(unit_price) OVER (ORDER BY moved_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_price,
        COUNT(*) OVER w_sku_id_warehouse_id AS rows_in_sku_id_warehouse_id,
        LAST_VALUE(qty_filled) IGNORE NULLS OVER (ORDER BY moved_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_qty_filled,
        SIZE(COLLECT_SET(operator) OVER w_sku_id_warehouse_id) AS distinct_operator_in_sku_id_warehouse_id
    FROM scoped_moves
    WINDOW w_sku_id_warehouse_id AS (PARTITION BY sku_id, warehouse_id)
),
per_group AS (
    -- Group-level totals
    SELECT
        warehouse_id,
        move_type,
        ROUND(STDDEV(qty), 2) AS stddev_qty,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list
    FROM with_windows
    GROUP BY warehouse_id, move_type
    HAVING MAX(first_unit_price) > 161.4
),
with_details_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.warehouse_id,
        s.move_type,
        s.stddev_qty,
        s.move_type_list,
        w.wh_code AS warehouse_wh_code,
        w.capacity AS warehouse_capacity,
        w.opened_on AS warehouse_opened_on
    FROM per_group s
    JOIN warehouses w ON w.id = s.warehouse_id
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.warehouse_id,
        s.move_type,
        s.stddev_qty,
        s.move_type_list,
        s.warehouse_wh_code,
        s.warehouse_capacity,
        s.warehouse_opened_on,
        w.wh_code AS warehouse_wh_code_2
    FROM with_details_warehouse s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
)
-- Final projection
SELECT
    warehouse_id,
    move_type,
    stddev_qty,
    move_type_list,
    warehouse_capacity,
    warehouse_opened_on,
    ROUND(stddev_qty * 0.25, 2) AS stddev_qty_adj
FROM joined
ORDER BY move_type
