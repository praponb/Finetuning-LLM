SELECT
    products.id,
    products.stock_qty,
    products.brand,
    suppliers.country,
    suppliers.rating,
    suppliers.city
FROM products
JOIN suppliers ON products.supplier_id = suppliers.id
ORDER BY city
