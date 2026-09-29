WITH account_stats AS (
    SELECT account_id, AVG(amount) avg_amount
    FROM transactions
    GROUP BY account_id
)
SELECT a.customer_id, a.balance, as.avg_amount
FROM accounts a
JOIN account_stats as ON a.id = as.account_id
WHERE as.avg_amount > 1600;
