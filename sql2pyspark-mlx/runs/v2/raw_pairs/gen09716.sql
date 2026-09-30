WITH account_totals AS (
    SELECT account_id, COUNT(amount) AS amount_count
    FROM transactions
    GROUP BY account_id
)
SELECT a.customer_id, at.amount_count
FROM accounts a
JOIN account_totals at ON a.id = at.account_id
ORDER BY amount_count, a.customer_id DESC
