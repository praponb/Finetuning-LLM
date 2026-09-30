SELECT suppliers.id, suppliers.rating
FROM suppliers
WHERE EXISTS (SELECT 1 FROM products WHERE products.supplier_id = suppliers.id AND products.cost BETWEEN 190 AND 310)
