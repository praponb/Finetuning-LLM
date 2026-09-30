WITH prepared_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        ROUND(unit_price / NULLIF(qty, 0), 3) AS unit_price_per_qty,
        TO_DATE(moved_at) AS moved_at_date
    FROM stock_moves
    WHERE (sku_id IS NOT NULL OR operator = 'op_eve')
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        unit_price_per_qty,
        moved_at_date,
        unit_price_per_qty - LAG(unit_price_per_qty) OVER (PARTITION BY move_type ORDER BY moved_at_date DESC, id) AS unit_price_per_qty_change,
        LEAD(unit_price) OVER (PARTITION BY move_type ORDER BY qty DESC, id) AS next_unit_price,
        FIRST_VALUE(moved_at) OVER (PARTITION BY move_type ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_moved_at
    FROM prepared_moves
)
-- Subtotals with GROUPING SETS
SELECT
    move_type,
    warehouse_id,
    SUM(unit_price_per_qty_change) AS total_unit_price_per_qty_change
FROM with_trends
GROUP BY GROUPING SETS ((move_type, warehouse_id), (move_type))
ORDER BY move_type NULLS LAST, warehouse_id NULLS LAST
