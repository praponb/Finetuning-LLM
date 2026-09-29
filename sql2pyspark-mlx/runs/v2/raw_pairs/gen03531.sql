WITH filtered_transactions AS (
    SELECT *
    FROM transactions
    WHERE account_id IN (3, 17) AND txn_type IN ('REFUND', 'FEE') AND is_flagged
),
TxnTypeSummary AS (
    SELECT txn_type, AVG(amount) AS avg_amount, COUNT(CASE WHEN merchant = 'Starbucks' THEN 1 END) AS starbucks_count
    FROM filtered_transactions
    GROUP BY txn_type
)
SELECT *
FROM TxnTypeSummary
WHERE starbucks_count < 4
ORDER BY avg_amount DESC
