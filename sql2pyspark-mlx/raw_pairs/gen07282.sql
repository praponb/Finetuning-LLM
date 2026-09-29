SELECT status, SUM(amount) AS total_amount, COUNT(*) AS cnt, DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS amount_rank FROM orders GROUP BY status
