SELECT
    p.product_name,
    ss.store_id,
    ss.product_id,
    ss.revenue
FROM products p
LEFT JOIN store_sales ss ON ss.product_id = p.id
ORDER BY revenue DESC;
