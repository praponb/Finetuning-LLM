WITH transactions_by_account AS (
    SELECT account_id, ROUND(STDDEV(amount), 2) AS stddev_amount, MIN(amount) AS min_amount
    FROM transactions
    WHERE txn_type IN ('REFUND', 'FEE', 'DEBIT')
    GROUP BY account_id
)
SELECT a.customer_id, tba.stddev_amount, tba.min_amount
FROM accounts a
LEFT JOIN transactions_by_account tba ON a.id = tba.account_id
