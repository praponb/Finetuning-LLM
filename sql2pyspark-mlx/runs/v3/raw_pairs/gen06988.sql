SELECT
    id,
    stock_qty,
    category,
    supplier_id,
    CAST(stock_qty AS DOUBLE) AS stock_qty_str
FROM products
WHERE LOWER(product_name) LIKE '%ire%';
