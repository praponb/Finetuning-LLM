SELECT ss.store_id, MAX(i.quantity) max_quantity, COUNT(i.quantity) quantity_count
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.store_id = 1
GROUP BY ss.store_id
ORDER BY max_quantity DESC, ss.store_id;
