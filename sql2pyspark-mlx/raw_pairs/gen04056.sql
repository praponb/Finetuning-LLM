SELECT products.id, products.supplier_id
FROM products
WHERE products.supplier_id IN (SELECT id FROM suppliers WHERE city = 'Berlin')
ORDER BY supplier_id, id
LIMIT 10;
