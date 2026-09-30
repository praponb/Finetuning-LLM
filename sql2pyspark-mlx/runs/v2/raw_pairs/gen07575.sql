SELECT merchant, ROUND(AVG(amount), 2) AS avg_amount, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(merchant))) AS merchant_values
FROM transactions
WHERE txn_type != 'FEE' AND txn_time < '2023-08-30' AND merchant != 'Amazon'
GROUP BY merchant
ORDER BY merchant_values DESC;
