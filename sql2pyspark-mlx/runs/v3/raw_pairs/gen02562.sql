SELECT product_id, avg_quantity FROM (SELECT product_id, AVG(quantity) avg_quantity FROM inventory GROUP BY product_id) agg WHERE avg_quantity > 230
