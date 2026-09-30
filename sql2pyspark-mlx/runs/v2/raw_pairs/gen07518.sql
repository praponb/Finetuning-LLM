WITH recent_transactions AS (
    SELECT *
    FROM transactions
    WHERE txn_type IN ('DEBIT', 'REFUND') AND txn_time < '2023-12-01'
),
MerchantSummary AS (
    SELECT merchant, SUM(amount) AS total_amount, SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list
    FROM recent_transactions
    GROUP BY merchant
)
SELECT *
FROM MerchantSummary
WHERE total_amount > 13000
ORDER BY total_amount DESC, merchant
LIMIT 20
