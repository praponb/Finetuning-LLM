SELECT c.segment, COUNT(CASE WHEN t.merchant = 'Lazada' THEN 1 END) AS lazada_count, SUM(CASE WHEN t.txn_type = 'DEBIT' THEN 1 ELSE 0 END) AS debit_count
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
INNER JOIN customers c ON c.id = a.customer_id
WHERE a.balance < 33000
GROUP BY c.segment
ORDER BY debit_count, c.segment DESC;
