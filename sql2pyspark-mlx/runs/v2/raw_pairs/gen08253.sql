SELECT ss.product_id, COUNT(DISTINCT i.quantity) AS unique_products, MAX(i.quantity) AS max_quantity
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY ss.product_id
ORDER BY unique_products DESC, product_id ASC
LIMIT 10
