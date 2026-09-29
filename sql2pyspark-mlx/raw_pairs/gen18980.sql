WITH account_stats AS (
    SELECT account_id, AVG(amount) AS avg_amount, SUM(CASE WHEN merchant = 'Starbucks' THEN amount ELSE 0 END) AS starbucks_amount
    FROM transactions
    WHERE account_id IN (8, 23, 27, 28)
    GROUP BY account_id
)
SELECT
    a.id,
    a.account_type,
    as.avg_amount,
    as.starbucks_amount
FROM accounts a
LEFT JOIN account_stats as ON a.id = as.account_id;
