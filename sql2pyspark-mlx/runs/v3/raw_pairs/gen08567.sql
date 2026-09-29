WITH filtered_transactions AS (
    SELECT *
    FROM transactions
    WHERE (txn_type NOT IN ('FEE', 'REFUND', 'DEBIT') AND merchant = 'Shell') OR account_id IN (9, 13, 15)
),
transactions_stats AS (
    SELECT DATE_TRUNC('MONTH', txn_time) AS txn_month, COUNT(CASE WHEN txn_type = 'FEE' THEN 1 END) AS fee_count, SUM(CASE WHEN txn_type = 'CREDIT' THEN amount ELSE 0 END) AS credit_amount
    FROM filtered_transactions
    GROUP BY DATE_TRUNC('MONTH', txn_time)
)
SELECT *
FROM transactions_stats;
