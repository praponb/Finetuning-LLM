SELECT HOUR(paid_at) AS paid_hour, SUM(amount) AS total_amount, DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS amount_rank
FROM payments
WHERE amount <= 1190
GROUP BY HOUR(paid_at)
