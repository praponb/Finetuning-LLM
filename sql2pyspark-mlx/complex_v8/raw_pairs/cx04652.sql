SELECT city, capacity AS entity_id, 'warehouse' AS source
FROM warehouses
UNION ALL
SELECT category AS city, id AS entity_id, 'sku' AS source
FROM skus
WHERE is_active = FALSE
UNION ALL
SELECT operator AS city, qty AS entity_id, 'move' AS source
FROM stock_moves
ORDER BY source
