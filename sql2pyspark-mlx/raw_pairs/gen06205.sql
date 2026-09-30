SELECT
    products.id,
    products.price,
    products.stock_qty,
    suppliers.supplier_name
FROM products
JOIN suppliers ON products.supplier_id = suppliers.id
ORDER BY products.id DESC;
