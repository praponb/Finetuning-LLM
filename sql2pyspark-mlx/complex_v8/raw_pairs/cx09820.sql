SELECT id AS entity_id, city AS name, 'warehouse' AS record_type
FROM warehouses
WHERE opened_on >= DATE '2024-02-19'
  AND LOWER(wh_code) LIKE '%l1%'
UNION ALL
SELECT warehouse_id, move_type, 'move' AS record_type
FROM stock_moves
WHERE moved_at >= TIMESTAMP '2025-02-12 10:00:00'
  AND warehouse_id >= 4
UNION ALL
SELECT id, sku_code, 'sku' AS record_type
FROM skus
ORDER BY record_type
