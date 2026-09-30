WITH transactions_by_account AS (
    SELECT account_id, COUNT(amount) AS amount_count, AVG(amount) AS avg_amount
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.status,
    a.balance,
    tba.amount_count,
    tba.avg_amount
FROM accounts a
INNER JOIN transactions_by_account tba ON a.id = tba.account_id
WHERE tba.avg_amount >= 1800
ORDER BY id
LIMIT 3
