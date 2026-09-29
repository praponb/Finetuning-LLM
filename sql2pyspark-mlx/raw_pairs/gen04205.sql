SELECT
    suppliers.id,
    suppliers.city,
    suppliers.rating,
    suppliers.supplier_name
FROM suppliers
WHERE EXISTS (SELECT 1 FROM products WHERE products.supplier_id = suppliers.id);
