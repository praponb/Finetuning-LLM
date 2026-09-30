SELECT account_id, COUNT(CASE WHEN merchant = 'Lazada' THEN 1 END) AS lazada_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) AS txn_type_values
FROM transactions
WHERE TO_DATE(txn_time) < '2023-09-01' AND amount BETWEEN 1900 AND 2200
GROUP BY account_id
HAVING lazada_count >= 5
ORDER BY account_id DESC, 3;
