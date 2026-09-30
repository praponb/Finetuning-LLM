WITH filtered_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price,
        DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE sku_id IS NOT NULL
      AND move_type <> 'OUTBOUND'
)
SELECT qty
FROM filtered_moves
WHERE move_type = 'ADJUST'
  AND warehouse_id >= 2
EXCEPT
SELECT id
FROM skus
ORDER BY qty DESC
