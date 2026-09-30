SELECT c.country, COUNT(DISTINCT a.balance) AS unique_account_types, COUNT(*) AS num_accounts
FROM accounts a
LEFT JOIN customers c ON c.id = a.customer_id
GROUP BY c.country
ORDER BY 2
