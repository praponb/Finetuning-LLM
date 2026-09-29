SELECT c.country, MIN(t.txn_time) AS min_txn_time, SUM(CASE WHEN t.txn_type = 'DEBIT' THEN 1 ELSE 0 END) AS debit_count
FROM transactions t
FULL OUTER JOIN accounts a ON t.account_id = a.id
JOIN customers c ON c.id = a.customer_id
GROUP BY c.country
ORDER BY min_txn_time, c.country
LIMIT 50
