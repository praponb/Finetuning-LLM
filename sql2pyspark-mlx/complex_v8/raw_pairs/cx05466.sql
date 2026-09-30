SELECT id AS qty, unit_price AS amount
FROM stock_moves
WHERE moved_at < TIMESTAMP '2025-03-02 10:00:00'
UNION
SELECT sku_id, unit_price
FROM stock_moves
WHERE sku_id >= 2
  AND operator = 'op_dew'
ORDER BY qty
