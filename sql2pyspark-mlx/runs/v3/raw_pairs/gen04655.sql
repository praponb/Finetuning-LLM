SELECT product_id, SUM(unit_price * quantity) AS revenue, COUNT(*) AS num_order_items FROM order_items GROUP BY product_id HAVING SUM(unit_price * quantity) > 77
