SELECT method, SUM(amount) AS total_amount, COUNT(*) AS cnt, DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS amount_rank FROM payments GROUP BY method
