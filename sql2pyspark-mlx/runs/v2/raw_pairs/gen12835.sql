SELECT product_id, MIN(quantity) AS min_quantity FROM inventory GROUP BY product_id HAVING MIN(quantity) < 110 ORDER BY min_quantity DESC, product_id;
