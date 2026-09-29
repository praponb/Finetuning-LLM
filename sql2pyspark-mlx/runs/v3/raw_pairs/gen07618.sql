WITH transactions_by_account AS (
    SELECT account_id, COUNT(amount) AS amount_count
    FROM transactions
    GROUP BY account_id
)
SELECT a.id, a.customer_id, tba.amount_count
FROM accounts a
INNER JOIN transactions_by_account tba ON a.id = tba.account_id
WHERE tba.amount_count > 4
ORDER BY tba.amount_count
