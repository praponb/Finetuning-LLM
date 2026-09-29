SELECT a.customer_id, SUM(CASE WHEN t.txn_type = 'DEBIT' THEN 1 ELSE 0 END) AS debit_count, SORT_ARRAY(COLLECT_SET(t.merchant)) AS merchant_list
FROM transactions t
LEFT JOIN accounts a ON a.id = t.account_id
WHERE a.balance <= 76000
GROUP BY a.customer_id
