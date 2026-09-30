SELECT suppliers.id, suppliers.rating
FROM suppliers
WHERE EXISTS (SELECT 1 FROM products WHERE products.supplier_id = suppliers.id AND products.cost > 720)
ORDER BY rating DESC, id
LIMIT 10;
