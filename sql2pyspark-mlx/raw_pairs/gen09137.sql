SELECT a.status, COUNT(*) AS total_count, COUNT(DISTINCT t.amount) AS unique_accounts
FROM transactions t
JOIN accounts a ON t.account_id = a.id
JOIN customers c ON a.customer_id = c.id
GROUP BY a.status
ORDER BY total_count DESC;
