SELECT ss.product_id, COUNT(i.quantity) quantity_count
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY ss.product_id;
