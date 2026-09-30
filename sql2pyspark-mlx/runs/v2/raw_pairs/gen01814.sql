SELECT status, SUM(discount) total_discount, RANK() OVER (ORDER BY SUM(discount) DESC) AS rnk FROM orders WHERE MONTH(order_date) IN (4, 7, 12) GROUP BY status;
