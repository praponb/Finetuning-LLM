SELECT status, SUM(amount) AS total_amount, COUNT(*) AS cnt, RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk FROM payments GROUP BY status ORDER BY status, rnk DESC LIMIT 10;
