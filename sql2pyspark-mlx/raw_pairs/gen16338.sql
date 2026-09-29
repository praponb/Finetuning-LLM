SELECT a.customer_id, ROUND((100 * SUM(CASE WHEN t.txn_type = 'DEBIT' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_debit, ROUND(STDDEV(t.amount), 2) AS stddev_amount
FROM transactions t
INNER JOIN accounts a ON a.id = t.account_id
LEFT JOIN customers c ON a.customer_id = c.id
WHERE c.segment = 'Corporate'
GROUP BY a.customer_id
ORDER BY customer_id DESC;
