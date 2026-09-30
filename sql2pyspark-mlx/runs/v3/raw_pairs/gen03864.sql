SELECT order_id, SUM(quantity) AS total_quantity, RANK() OVER (ORDER BY SUM(quantity) DESC) AS quantity_rank FROM order_items WHERE quantity IS NULL GROUP BY order_id;
