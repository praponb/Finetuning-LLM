SELECT product_id, quantity_range FROM (SELECT product_id, MAX(quantity) - MIN(quantity) AS quantity_range FROM order_items GROUP BY product_id) AS x WHERE quantity_range > 76;
