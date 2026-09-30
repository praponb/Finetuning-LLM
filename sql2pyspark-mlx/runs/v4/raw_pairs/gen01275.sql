SELECT method, SUM(amount) total_amount, COUNT(*) cnt, DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk FROM payments WHERE order_id IS NOT NULL GROUP BY method;
