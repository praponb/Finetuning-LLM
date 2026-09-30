SELECT ss.product_id, MAX(i.quantity) AS max_quantity
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY ss.product_id
ORDER BY product_id DESC
LIMIT 10;
