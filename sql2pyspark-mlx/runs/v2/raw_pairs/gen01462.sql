WITH recent_transactions AS (
    SELECT *
    FROM transactions
    WHERE amount < 2600 AND HOUR(txn_time) BETWEEN 12 AND 19 AND merchant NOT IN ('Amazon', '7-Eleven')
),
summary AS (
    SELECT merchant, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) AS txn_type_values
    FROM recent_transactions
    GROUP BY merchant
)
SELECT *
FROM summary
ORDER BY merchant DESC, txn_type_values DESC;
