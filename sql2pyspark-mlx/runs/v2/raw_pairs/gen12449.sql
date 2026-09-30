SELECT
    products.id,
    products.product_name,
    suppliers.city,
    suppliers.supplier_name
FROM products
RIGHT OUTER JOIN suppliers ON suppliers.id = products.supplier_id
ORDER BY products.product_name;
