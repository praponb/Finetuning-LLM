WITH transactions_by_account AS (
    SELECT account_id, MAX(txn_time) AS latest_txn_time, ROUND(STDDEV(amount), 2) AS stddev_amount
    FROM transactions
    GROUP BY account_id
)
SELECT a.customer_id, tba.latest_txn_time, tba.stddev_amount
FROM accounts a
LEFT JOIN transactions_by_account tba ON a.id = tba.account_id
WHERE tba.stddev_amount >= 67;
