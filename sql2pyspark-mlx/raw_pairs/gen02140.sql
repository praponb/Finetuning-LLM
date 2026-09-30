WITH base AS (
    SELECT *
    FROM transactions
    WHERE account_id IN (15, 28) AND txn_type = 'REFUND'
),
TransactionsStats AS (
    SELECT CASE WHEN YEAR(txn_time) < 2024 THEN 'before_2024' WHEN YEAR(txn_time) = 2024 THEN '2024' ELSE 'after_2024' END AS txn_cohort, COUNT(*) AS transaction_count
    FROM base
    GROUP BY CASE WHEN YEAR(txn_time) < 2024 THEN 'before_2024' WHEN YEAR(txn_time) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT txn_cohort, transaction_count
FROM TransactionsStats
ORDER BY transaction_count;
