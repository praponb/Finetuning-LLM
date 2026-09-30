SELECT products.id, products.brand, products.category
FROM products
WHERE products.supplier_id = 5 AND products.supplier_id NOT IN (SELECT id FROM suppliers WHERE LOWER(supplier_name) LIKE '%ion%')
ORDER BY id ASC, category DESC
