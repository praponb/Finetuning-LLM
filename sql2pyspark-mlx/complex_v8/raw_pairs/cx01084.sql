SELECT warehouse_id AS key_id, unit_price, 'move' AS origin
FROM stock_moves
WHERE qty BETWEEN 145 AND 393
  AND moved_at < TIMESTAMP '2025-02-18 15:00:00'
UNION ALL
SELECT id, unit_cost, 'sku' AS origin
FROM skus
WHERE NOT is_active
  AND unit_cost > 108.5
