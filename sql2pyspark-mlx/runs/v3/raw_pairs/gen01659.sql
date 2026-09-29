SELECT merchant, SUM(amount) AS total_amount, DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS amount_rank
FROM transactions
WHERE account_id IN (15, 18)
GROUP BY merchant
