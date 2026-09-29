SELECT HOUR(txn_time) AS txn_hour, SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list
FROM transactions
GROUP BY 1
ORDER BY 1;
