SELECT p.id, p.brand
FROM products p
WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE city IN ('London', 'Tokyo', 'Berlin'))
ORDER BY p.id
LIMIT 1
