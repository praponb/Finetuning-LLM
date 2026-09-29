SELECT product_id, store_id, SUM(quantity) AS total_quantity FROM inventory GROUP BY product_id, store_id ORDER BY store_id DESC, product_id LIMIT 20;
