SELECT suppliers.id, suppliers.city, suppliers.supplier_name
FROM suppliers
WHERE NOT EXISTS (SELECT 1 FROM products WHERE products.supplier_id = suppliers.id AND products.price > 650)
ORDER BY suppliers.id DESC;
