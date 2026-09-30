SELECT t.account_id, a.customer_id, c.city
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
JOIN customers c ON a.customer_id = c.id
