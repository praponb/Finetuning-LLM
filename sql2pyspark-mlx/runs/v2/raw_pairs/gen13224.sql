SELECT
    products.id,
    products.product_name,
    products.stock_qty,
    products.supplier_id,
    suppliers.rating,
    suppliers.city
FROM products
RIGHT JOIN suppliers ON products.supplier_id = suppliers.id
ORDER BY products.id, suppliers.city;
