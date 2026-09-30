WITH base AS (
    -- Filter stock_moves
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
    WHERE move_type IS NOT NULL
      AND qty <= 268
),
with_parent_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.unit_price,
        s.qty_filled,
        w.capacity AS warehouse_capacity
    FROM base s
    JOIN warehouses w ON w.id = s.warehouse_id
    WHERE w.capacity < 11768
),
summary AS (
    -- Group-level totals
    SELECT
        operator,
        move_type,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list,
        COUNT(*) AS cnt
    FROM with_parent_warehouse
    GROUP BY 1, 2
    HAVING COUNT(*) >= 2
)
SELECT operator, move_type, cnt
FROM summary
ORDER BY operator DESC, move_type
