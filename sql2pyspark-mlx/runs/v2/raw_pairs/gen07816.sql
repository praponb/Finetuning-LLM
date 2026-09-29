SELECT c.city, COUNT(CASE WHEN t.txn_type = 'REFUND' THEN 1 END) AS refund_count
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
INNER JOIN customers c ON a.customer_id = c.id
GROUP BY c.city;
