WITH account_summary AS (
    SELECT account_id, MAX(amount) - MIN(amount) AS amount_range
    FROM transactions
    WHERE account_id IN (16, 20, 22, 26)
    GROUP BY account_id
)
SELECT a.customer_id, a.status, as.amount_range
FROM accounts a
LEFT JOIN account_summary as ON a.id = as.account_id
