SELECT *
FROM (
    SELECT txn_type, MAX(amount) AS max_amount, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(merchant))) AS merchant_values
    FROM transactions
    GROUP BY txn_type
) AS t
WHERE max_amount < 2000;
