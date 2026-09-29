SELECT
    products.id,
    products.stock_qty,
    suppliers.city,
    suppliers.rating
FROM products
INNER JOIN suppliers ON products.supplier_id = suppliers.id;
