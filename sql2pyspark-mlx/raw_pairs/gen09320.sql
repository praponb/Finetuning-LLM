WITH filtered_transactions AS (
    SELECT *, COALESCE(merchant, 'N/A') AS merchant_filled
    FROM transactions
    WHERE merchant = 'Grab' AND account_id IN (1, 4, 20, 33)
),
txn_type_summary AS (
    SELECT txn_type, ROUND(STDDEV(amount), 2) AS stddev_amount
    FROM filtered_transactions
    GROUP BY txn_type
)
SELECT txn_type, stddev_amount
FROM txn_type_summary
ORDER BY stddev_amount, txn_type
LIMIT 3
