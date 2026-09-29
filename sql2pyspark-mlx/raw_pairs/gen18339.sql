SELECT ss.store_id, COUNT(i.quantity) AS quantity_count FROM inventory i LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id GROUP BY ss.store_id ORDER BY store_id;
