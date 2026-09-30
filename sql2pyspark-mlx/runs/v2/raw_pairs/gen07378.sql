WITH account_summary AS (
    SELECT account_id, COUNT(amount) AS amount_count
    FROM transactions
    WHERE amount IS NOT NULL
    GROUP BY account_id
)
SELECT
    a.id,
    a.balance,
    a.opened_date,
    as.amount_count
FROM accounts a
LEFT JOIN account_summary as ON a.id = as.account_id
ORDER BY amount_count DESC
