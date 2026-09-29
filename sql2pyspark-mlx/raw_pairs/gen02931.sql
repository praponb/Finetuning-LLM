SELECT c.is_active, COUNT(*) AS cnt FROM transactions t FULL OUTER JOIN accounts a ON a.id = t.account_id LEFT OUTER JOIN customers c ON c.id = a.customer_id GROUP BY c.is_active ORDER BY cnt DESC;
