WITH account_totals AS (
    SELECT account_id, COUNT(amount) AS amount_count
    FROM transactions
    GROUP BY account_id
)
SELECT a.opened_date, at.amount_count
FROM accounts a
INNER JOIN account_totals at ON a.id = at.account_id
ORDER BY at.amount_count DESC
