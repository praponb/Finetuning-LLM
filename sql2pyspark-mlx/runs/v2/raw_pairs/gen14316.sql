WITH filtered_transactions AS (
    SELECT *
    FROM transactions
    WHERE merchant IN ('Shell', 'Lazada') AND merchant IS NOT NULL
),
merchant_summary AS (
    SELECT merchant, COUNT(CASE WHEN txn_type = 'REFUND' THEN 1 END) AS refund_count
    FROM filtered_transactions
    GROUP BY merchant
)
SELECT merchant, refund_count
FROM merchant_summary;
