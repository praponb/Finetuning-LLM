SELECT suppliers.id, suppliers.city
FROM suppliers
WHERE suppliers.city = 'Berlin' AND EXISTS (SELECT 1 FROM products WHERE products.supplier_id = suppliers.id)
