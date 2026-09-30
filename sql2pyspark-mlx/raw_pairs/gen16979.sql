SELECT
    p.id,
    p.cost,
    p.supplier_id,
    p.brand
FROM products p
WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE city IN ('London', 'Tokyo', 'Berlin'))
