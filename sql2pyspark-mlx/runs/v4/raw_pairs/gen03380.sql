SELECT HOUR(txn_time) txn_hour, SORT_ARRAY(COLLECT_SET(txn_type)) txn_type_list, MIN(amount) min_amount
FROM transactions
GROUP BY 1
HAVING MIN(amount) < 3800
ORDER BY min_amount
