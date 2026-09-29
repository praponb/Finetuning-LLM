SELECT product_id, avg_quantity FROM (SELECT product_id, AVG(quantity) AS avg_quantity FROM inventory GROUP BY product_id) AS s WHERE avg_quantity < 160 ORDER BY product_id DESC, avg_quantity;
