WITH base AS (
    SELECT *, CONCAT_WS('-', merchant, txn_type) AS merchant_txn_type_key
    FROM transactions
    WHERE txn_type = 'CREDIT' AND amount >= 3900
),
summary AS (
    SELECT MONTH(txn_time) AS txn_month, ROUND(STDDEV(amount), 2) AS stddev_amount, MAX(amount) AS max_amount
    FROM base
    GROUP BY MONTH(txn_time)
)
SELECT txn_month, max_amount
FROM summary
WHERE max_amount >= 1800
