SELECT product_id, SUM(quantity) total_quantity, COUNT(*) cnt, RANK() OVER (ORDER BY SUM(quantity) DESC) AS rnk FROM order_items GROUP BY product_id;
