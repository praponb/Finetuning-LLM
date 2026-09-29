SELECT p.id, p.stock_qty, p.cost
FROM products p
WHERE p.stock_qty >= 396 AND p.supplier_id NOT IN (SELECT id FROM suppliers WHERE city != 'Berlin')
ORDER BY p.id, cost;
