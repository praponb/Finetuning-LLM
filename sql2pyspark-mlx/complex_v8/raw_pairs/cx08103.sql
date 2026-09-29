WITH filtered AS (
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
    WHERE qty <= 372
      AND unit_price <= 298.9
)
SELECT move_type AS category, operator, unit_price AS metric
FROM filtered
WHERE MONTH(moved_at) IN (1, 2)
  AND operator NOT IN ('op_bee', 'op_eve')
UNION ALL
SELECT operator AS category, move_type AS operator, unit_price AS metric
FROM stock_moves
ORDER BY operator
