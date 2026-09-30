SELECT a.id, a.opened_date
FROM accounts a
WHERE a.opened_date < '2025-09-01' AND a.customer_id IN (SELECT id FROM customers WHERE credit_limit < 13500)
ORDER BY a.id, opened_date DESC
