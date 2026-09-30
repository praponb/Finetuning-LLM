SELECT
    suppliers.id,
    suppliers.rating,
    suppliers.supplier_name,
    suppliers.country
FROM suppliers
WHERE NOT EXISTS (SELECT 1 FROM products WHERE products.supplier_id = suppliers.id AND LOWER(TRIM(products.product_name)) = 'water bottle')
