WITH transactions_by_account AS (
    SELECT account_id, ROUND(AVG(amount), 2) AS avg_amount
    FROM transactions
    WHERE TO_DATE(txn_time) >= '2024-06-18'
    GROUP BY account_id
)
SELECT a.id, a.status, tba.avg_amount
FROM accounts a
LEFT OUTER JOIN transactions_by_account tba ON a.id = tba.account_id
