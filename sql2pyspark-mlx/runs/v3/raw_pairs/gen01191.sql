SELECT MONTH(txn_time) AS txn_month, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) AS txn_type_values, SUM(CASE WHEN merchant = 'Amazon' THEN 1 ELSE 0 END) AS amazon_count
FROM transactions
GROUP BY MONTH(txn_time)
HAVING SUM(CASE WHEN merchant = 'Amazon' THEN 1 ELSE 0 END) >= 22
