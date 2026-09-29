WITH account_stats AS (
    SELECT account_id, COUNT(DISTINCT amount) AS unique_merchants
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.balance,
    a.account_type,
    as.unique_merchants
FROM accounts a
INNER JOIN account_stats as ON a.id = as.account_id
WHERE as.unique_merchants > 9;
