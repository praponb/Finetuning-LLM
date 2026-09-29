SELECT qty AS ref_id, moved_at
FROM stock_moves
WHERE qty > 91
  AND warehouse_id IS NOT NULL
UNION ALL
SELECT sku_id AS ref_id, moved_at
FROM stock_moves
WHERE unit_price BETWEEN 119.5 AND 393.3
ORDER BY ref_id, moved_at
