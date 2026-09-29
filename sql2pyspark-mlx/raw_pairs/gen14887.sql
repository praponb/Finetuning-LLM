WITH remaining_keys AS (
    SELECT id, merchant
    FROM transactions
    INTERSECT ALL
    SELECT id, merchant
    FROM transactions
    WHERE merchant IN ('Amazon', 'Shell')
),
kept_rows AS (
    SELECT s.*
    FROM transactions s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.merchant = k.merchant
)
SELECT
    CASE WHEN amount >= 4500 THEN 'LARGE' ELSE 'SMALL' END AS amount_tier,
    COUNT_IF(txn_type = 'CREDIT') AS credit_events,
    SUM(CASE WHEN txn_type = 'REFUND' THEN amount ELSE 0 END) AS refund_amount
FROM kept_rows
GROUP BY CASE WHEN amount >= 4500 THEN 'LARGE' ELSE 'SMALL' END
