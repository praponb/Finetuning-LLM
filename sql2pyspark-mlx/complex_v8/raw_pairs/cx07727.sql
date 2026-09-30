WITH base AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        DATE_FORMAT(moved_at, 'MMM yyyy') AS moved_at_label
    FROM stock_moves
    WHERE MONTH(moved_at) IN (2, 3)
)
SELECT
    operator AS name,
    unit_price,
    moved_at,
    'move' AS src
FROM base
WHERE unit_price >= 119.5
UNION ALL
SELECT
    operator,
    unit_price,
    moved_at,
    'move_2' AS src
FROM stock_moves
