SELECT ss.id, ss.revenue
FROM store_sales ss
WHERE ss.revenue < 3500 AND ss.product_id IN (SELECT id FROM products WHERE product_name LIKE '%Head%')
ORDER BY ss.revenue DESC
