WITH filtered_transactions AS (
    SELECT *, LENGTH(merchant) merchant_length
    FROM transactions
    WHERE (txn_time < '2024-08-01' AND txn_type IN ('REFUND', 'DEBIT', 'CREDIT')) OR merchant IN ('Amazon', 'Shell')
),
transactions_stats AS (
    SELECT is_flagged, ROUND(AVG(amount), 2) avg_amount
    FROM filtered_transactions
    GROUP BY is_flagged
)
SELECT *
FROM transactions_stats
WHERE avg_amount >= 33
ORDER BY avg_amount DESC
