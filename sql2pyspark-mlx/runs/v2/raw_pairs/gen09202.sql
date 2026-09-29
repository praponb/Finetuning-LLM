SELECT c.is_active, a.balance, a.opened_date
FROM customers c
LEFT JOIN accounts a ON a.customer_id = c.id
WHERE c.credit_limit > 10500
ORDER BY balance DESC
