WITH account_totals AS (
    SELECT account_id, ROUND(AVG(amount), 2) AS avg_amount, COUNT(amount) AS amount_count
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.opened_date,
    at.avg_amount,
    at.amount_count
FROM accounts a
LEFT JOIN account_totals at ON a.id = at.account_id
WHERE at.avg_amount > 22
ORDER BY amount_count DESC, id DESC;
