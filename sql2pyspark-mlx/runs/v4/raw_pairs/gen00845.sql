SELECT
    products.brand,
    products.supplier_id,
    products.price,
    suppliers.country
FROM products
INNER JOIN suppliers ON products.supplier_id = suppliers.id
WHERE suppliers.supplier_name LIKE 'G%'
ORDER BY products.price DESC, 4 DESC
