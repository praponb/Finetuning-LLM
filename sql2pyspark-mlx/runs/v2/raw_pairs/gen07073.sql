SELECT suppliers.id, suppliers.country, suppliers.rating
FROM suppliers
WHERE suppliers.rating < 4.2 AND NOT EXISTS (SELECT 1 FROM products WHERE products.supplier_id = suppliers.id)
ORDER BY suppliers.id;
