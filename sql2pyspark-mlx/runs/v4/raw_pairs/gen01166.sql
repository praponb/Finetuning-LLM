WITH transactions_by_account AS (
    SELECT account_id, ROUND(AVG(amount), 2) avg_amount
    FROM transactions
    GROUP BY account_id
)
SELECT a.balance, tba.avg_amount
FROM accounts a
INNER JOIN transactions_by_account tba ON a.id = tba.account_id
WHERE tba.avg_amount >= 44
