WITH base AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        CASE WHEN operator IN ('op_dew', 'op_ann') THEN 'group_a' WHEN operator = 'op_eve' THEN 'group_b' ELSE 'other' END AS operator_group,
        GREATEST(qty, unit_price) AS greatest_qty_unit_price
    FROM stock_moves
    WHERE unit_price < 361.9
),
totals AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month,
        warehouse_id,
        CASE WHEN qty >= 300 THEN 'HIGH' WHEN qty >= 111 THEN 'MEDIUM' ELSE 'LOW' END AS qty_tier,
        MAX(greatest_qty_unit_price) AS max_greatest_qty_unit_price,
        ROUND(SUM(greatest_qty_unit_price), 2) AS total_greatest_qty_unit_price,
        MAX(moved_at) AS last_moved_at,
        ROUND(100 * COUNT(CASE WHEN move_type <> 'TRANSFER' THEN 1 END) / COUNT(*), 1) AS pct_move_type_transfer
    FROM base
    WHERE move_type IN ('ADJUST', 'INBOUND')
    GROUP BY DATE_TRUNC('MONTH', moved_at), warehouse_id, CASE WHEN qty >= 300 THEN 'HIGH' WHEN qty >= 111 THEN 'MEDIUM' ELSE 'LOW' END
    HAVING SUM(greatest_qty_unit_price) > 200.6
),
with_parent_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.*,
        w.city AS warehouse_city,
        w.capacity AS warehouse_capacity,
        w.wh_code AS warehouse_wh_code
    FROM totals s
    JOIN warehouses w ON w.id = s.warehouse_id
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(last_moved_at) - UNIX_TIMESTAMP(moved_at_month)) / 60, 1) AS minutes_moved_at_month_to_last_moved_at
    FROM with_parent_warehouse
)
SELECT
    moved_at_month,
    warehouse_id,
    total_greatest_qty_unit_price,
    last_moved_at,
    pct_move_type_transfer,
    warehouse_city,
    warehouse_wh_code,
    minutes_moved_at_month_to_last_moved_at
FROM annotated
ORDER BY total_greatest_qty_unit_price NULLS LAST, warehouse_id
