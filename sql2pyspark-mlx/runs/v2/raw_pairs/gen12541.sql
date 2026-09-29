SELECT
    p.category,
    p.product_name,
    p.launch_date,
    ss.sale_date,
    ss.units,
    ss.revenue
FROM products p
INNER JOIN store_sales ss ON ss.product_id = p.id
WHERE p.stock_qty <= 291 AND ss.units < 14
ORDER BY p.launch_date ASC
