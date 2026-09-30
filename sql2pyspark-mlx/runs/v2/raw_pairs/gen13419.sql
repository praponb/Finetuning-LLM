SELECT order_id, SUM(quantity) AS total_quantity, DENSE_RANK() OVER (ORDER BY SUM(quantity) DESC) AS quantity_rank FROM order_items GROUP BY order_id ORDER BY total_quantity DESC;
