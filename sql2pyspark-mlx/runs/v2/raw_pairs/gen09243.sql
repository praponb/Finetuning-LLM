WITH account_totals AS (
    SELECT account_id, COUNT(DISTINCT amount) AS unique_merchants
    FROM transactions
    GROUP BY account_id
)
SELECT a.id, a.account_type, at.unique_merchants
FROM accounts a
LEFT JOIN account_totals at ON a.id = at.account_id
