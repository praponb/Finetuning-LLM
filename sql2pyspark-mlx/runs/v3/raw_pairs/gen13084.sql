SELECT merchant, credit_count, txn_type_list
FROM (
    SELECT merchant, SUM(CASE WHEN txn_type = 'CREDIT' THEN 1 ELSE 0 END) AS credit_count, SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list
    FROM transactions
    GROUP BY merchant
) AS s
WHERE credit_count > 61
