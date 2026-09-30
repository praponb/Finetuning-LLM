SELECT TO_DATE(txn_time) AS txn_day, SUM(CASE WHEN merchant = 'Starbucks' THEN 1 ELSE 0 END) AS starbucks_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) AS txn_type_values
FROM transactions
WHERE merchant != 'Starbucks' AND account_id IN (17, 24, 26, 28)
GROUP BY TO_DATE(txn_time)
HAVING starbucks_count < 56
