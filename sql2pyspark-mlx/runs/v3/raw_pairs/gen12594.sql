SELECT c.is_active, ROUND((100 * SUM(CASE WHEN t.txn_type = 'DEBIT' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_debit
FROM transactions t
LEFT OUTER JOIN accounts a ON t.account_id = a.id
FULL OUTER JOIN customers c ON a.customer_id = c.id
GROUP BY c.is_active
HAVING ROUND((100 * SUM(CASE WHEN t.txn_type = 'DEBIT' THEN 1 ELSE 0 END)) / COUNT(*), 1) >= 67
ORDER BY pct_debit DESC;
