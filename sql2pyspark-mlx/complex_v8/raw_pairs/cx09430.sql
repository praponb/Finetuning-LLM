SELECT sku_id
FROM stock_moves
EXCEPT
SELECT sku_id
FROM stock_moves
WHERE MONTH(moved_at) IN (2, 3)
  AND qty < 288
