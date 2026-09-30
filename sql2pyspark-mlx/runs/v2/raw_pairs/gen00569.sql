SELECT * FROM (SELECT product_id, MAX(quantity) - MIN(quantity) AS quantity_range, SUM(quantity) AS total_quantity FROM order_items GROUP BY product_id) AS agg WHERE quantity_range >= 63;
