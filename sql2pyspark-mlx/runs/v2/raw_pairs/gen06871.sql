SELECT MONTH(txn_time) AS txn_month, txn_type, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) AS txn_type_values
FROM transactions
WHERE merchant != 'Amazon' AND amount <= 2900
GROUP BY MONTH(txn_time), txn_type
ORDER BY txn_month DESC
