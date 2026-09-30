WITH valid_transactions AS (
    SELECT *
    FROM transactions
    WHERE account_id IN (8, 24, 38)
),
Summary AS (
    SELECT
        merchant,
        DATE_TRUNC('MONTH', txn_time) AS txn_month,
        MAX(amount) AS max_amount,
        MAX(amount) - MIN(amount) AS amount_range
    FROM valid_transactions
    GROUP BY merchant, DATE_TRUNC('MONTH', txn_time)
)
SELECT
    merchant,
    txn_month,
    max_amount,
    amount_range
FROM Summary
ORDER BY merchant DESC
