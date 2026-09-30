SELECT
    p.category,
    p.stock_qty,
    ss.store_id,
    ss.sale_date
FROM products p
LEFT JOIN store_sales ss ON ss.product_id = p.id
ORDER BY p.category DESC;
