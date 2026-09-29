SELECT customer_id, MIN(balance) min_balance
FROM accounts
GROUP BY customer_id
ORDER BY min_balance DESC
