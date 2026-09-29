SELECT method, SUM(amount) total_amount, DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS amount_rank FROM payments GROUP BY method ORDER BY amount_rank DESC, method ASC LIMIT 5;
