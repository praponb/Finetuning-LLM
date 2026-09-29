WITH account_stats AS (
    SELECT account_id, AVG(amount) AS avg_amount, COUNT(*) AS num_transactions
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.opened_date,
    a.status,
    as.avg_amount,
    as.num_transactions
FROM accounts a
LEFT OUTER JOIN account_stats as ON a.id = as.account_id
WHERE as.num_transactions > 9
