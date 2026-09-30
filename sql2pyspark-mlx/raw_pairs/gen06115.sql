WITH transactions_by_account AS (
    SELECT account_id, ROUND(AVG(amount), 2) AS avg_amount, SUM(CASE WHEN txn_type = 'CREDIT' THEN amount ELSE 0 END) AS credit_amount
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.customer_id,
    a.balance,
    tba.avg_amount,
    tba.credit_amount
FROM accounts a
INNER JOIN transactions_by_account tba ON a.id = tba.account_id
WHERE tba.avg_amount >= 44
