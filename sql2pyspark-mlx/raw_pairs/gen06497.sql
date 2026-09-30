WITH account_summary AS (
    SELECT account_id, AVG(amount) AS avg_amount, MAX(amount) - MIN(amount) AS amount_range
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.balance,
    as.avg_amount,
    as.amount_range
FROM accounts a
JOIN account_summary as ON a.id = as.account_id
WHERE as.amount_range >= 72;
