SELECT ss.store_id, MIN(i.quantity) AS min_quantity
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY ss.store_id
ORDER BY min_quantity DESC, store_id
LIMIT 10
