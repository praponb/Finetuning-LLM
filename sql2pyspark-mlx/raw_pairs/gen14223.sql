WITH account_totals AS (
    SELECT account_id, AVG(amount) AS avg_amount, MAX(amount) - MIN(amount) AS amount_range
    FROM transactions
    WHERE NOT is_flagged
    GROUP BY account_id
)
SELECT
    a.id,
    a.opened_date,
    at.avg_amount,
    at.amount_range
FROM accounts a
JOIN account_totals at ON a.id = at.account_id
WHERE at.avg_amount > 1800
ORDER BY amount_range ASC, at.avg_amount ASC, id ASC
LIMIT 50;
