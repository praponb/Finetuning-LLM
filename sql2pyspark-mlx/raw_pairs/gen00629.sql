SELECT *
FROM (
    SELECT
        DATE_TRUNC('MONTH', txn_time) AS txn_month,
        merchant,
        SUM(amount) AS total_amount,
        MIN(amount) AS min_amount
    FROM transactions
    GROUP BY DATE_TRUNC('MONTH', txn_time), merchant
) AS sub
WHERE total_amount >= 23000
