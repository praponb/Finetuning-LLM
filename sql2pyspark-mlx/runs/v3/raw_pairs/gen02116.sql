SELECT c.id, c.email, COUNT(a.id) AS account_count
FROM customers c
LEFT JOIN accounts a ON a.customer_id = c.id
WHERE c.credit_limit >= 8500
GROUP BY c.id, c.email;
