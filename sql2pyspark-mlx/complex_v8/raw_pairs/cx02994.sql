WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM stock_moves
    WHERE HOUR(moved_at) BETWEEN 8 AND 12
      AND warehouse_id IS NOT NULL
)
SELECT
    warehouse_id,
    unit_price AS metric,
    qty,
    'move' AS origin
FROM recent
UNION ALL
SELECT
    id,
    unit_price,
    sku_id,
    'move_2' AS origin
FROM stock_moves
