WITH filtered AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price
    FROM stock_moves
    WHERE MONTH(moved_at) IN (1, 3)
)
SELECT warehouse_id AS qty
FROM filtered
WHERE unit_price BETWEEN 136.0 AND 361.9
  AND sku_id IS NOT NULL
EXCEPT
SELECT qty
FROM stock_moves
ORDER BY qty
