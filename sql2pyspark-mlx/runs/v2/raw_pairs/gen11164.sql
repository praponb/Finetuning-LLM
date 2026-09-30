SELECT status, SUM(discount) AS total_discount, RANK() OVER (ORDER BY SUM(discount) DESC) AS rnk FROM orders GROUP BY status ORDER BY total_discount DESC
