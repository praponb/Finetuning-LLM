WITH transactions_by_account AS (
    SELECT account_id, MAX(amount) AS max_amount, MAX(amount) - MIN(amount) AS amount_range
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.customer_id,
    tba.max_amount,
    tba.amount_range
FROM accounts a
LEFT JOIN transactions_by_account tba ON a.id = tba.account_id
WHERE tba.amount_range >= 39
