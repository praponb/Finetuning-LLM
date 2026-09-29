SELECT suppliers.id, suppliers.rating, suppliers.supplier_name
FROM suppliers
WHERE NOT EXISTS (SELECT 1 FROM products WHERE products.supplier_id = suppliers.id AND products.price IS NOT NULL)
