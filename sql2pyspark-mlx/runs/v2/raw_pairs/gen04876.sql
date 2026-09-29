SELECT c.name, c.email, a.balance
FROM customers c
LEFT JOIN accounts a ON c.id = a.customer_id
WHERE c.credit_limit > 13400 AND MONTH(a.opened_date) IN (3, 4, 9)
ORDER BY balance DESC
