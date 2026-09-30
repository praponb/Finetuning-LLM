SELECT id AS ref_id, capacity AS ref_id_2
FROM warehouses
UNION ALL
SELECT warehouse_id, sku_id
FROM stock_moves
UNION ALL
SELECT capacity, id
FROM warehouses
WHERE city NOT IN ('Bangkok', 'Kuala Lumpur')
  AND opened_on >= DATE '2024-04-13'
ORDER BY ref_id
