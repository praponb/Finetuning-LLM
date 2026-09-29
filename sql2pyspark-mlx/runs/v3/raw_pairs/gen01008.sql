WITH recent_transactions AS (
    SELECT *
    FROM transactions
    WHERE merchant IN ('Shell', 'Grab') AND amount < 2500
),
Summary AS (
    SELECT CASE WHEN YEAR(txn_time) < 2024 THEN 'before_2024' WHEN YEAR(txn_time) = 2024 THEN '2024' ELSE 'after_2024' END AS txn_cohort, SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list, MAX(amount) - MIN(amount) AS amount_range
    FROM recent_transactions
    GROUP BY CASE WHEN YEAR(txn_time) < 2024 THEN 'before_2024' WHEN YEAR(txn_time) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT txn_cohort, merchant_list, amount_range
FROM Summary
