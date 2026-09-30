WITH base AS (
    SELECT *
    FROM transactions
    WHERE txn_type = 'REFUND'
),
summary AS (
    SELECT txn_type, SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list, AVG(amount) AS avg_amount
    FROM base
    GROUP BY txn_type
)
SELECT txn_type, txn_type_list, avg_amount
FROM summary
WHERE avg_amount >= 2400;
