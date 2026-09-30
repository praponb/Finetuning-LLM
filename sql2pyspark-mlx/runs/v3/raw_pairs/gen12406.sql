WITH valid_transactions AS (
    SELECT *
    FROM transactions
    WHERE txn_time >= '2023-11-12' AND txn_type IN ('DEBIT', 'REFUND', 'CREDIT')
),
txn_month_summary AS (
    SELECT DATE_TRUNC('MONTH', txn_time) AS txn_month, MAX(amount) AS max_amount, COUNT(*) AS total_count
    FROM valid_transactions
    GROUP BY DATE_TRUNC('MONTH', txn_time)
)
SELECT txn_month, total_count
FROM txn_month_summary
WHERE max_amount >= 2000;
