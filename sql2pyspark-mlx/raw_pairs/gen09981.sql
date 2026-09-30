WITH recent_transactions AS (
    SELECT *
    FROM transactions
    WHERE account_id IN (21, 24, 25)
),
agg AS (
    SELECT txn_type, MAX(amount) max_amount, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) txn_type_values
    FROM recent_transactions
    GROUP BY txn_type
)
SELECT *
FROM agg
WHERE max_amount >= 2600
ORDER BY 3 DESC;
