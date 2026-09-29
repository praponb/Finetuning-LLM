SELECT order_id, SUM(quantity) AS total_quantity, COUNT(*) AS cnt, RANK() OVER (ORDER BY SUM(quantity) DESC) AS rnk FROM order_items WHERE unit_price < 240 GROUP BY order_id;
