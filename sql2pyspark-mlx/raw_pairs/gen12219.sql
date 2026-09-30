SELECT
    products.id,
    products.brand,
    products.cost,
    products.product_name,
    suppliers.city,
    suppliers.rating,
    suppliers.supplier_name
FROM products
INNER JOIN suppliers ON products.supplier_id = suppliers.id
ORDER BY rating DESC
