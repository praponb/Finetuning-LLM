WITH transactions_by_account AS (
    SELECT account_id, COUNT(DISTINCT amount) unique_merchants, ROUND(STDDEV(amount), 2) stddev_amount
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.customer_id,
    tba.unique_merchants,
    tba.stddev_amount
FROM accounts a
LEFT JOIN transactions_by_account tba ON a.id = tba.account_id
ORDER BY a.customer_id
