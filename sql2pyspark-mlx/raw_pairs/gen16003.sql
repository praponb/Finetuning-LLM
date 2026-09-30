SELECT a.id, a.status, a.opened_date
FROM accounts a
WHERE a.customer_id NOT IN (SELECT id FROM customers WHERE credit_limit BETWEEN 7500 AND 11600)
