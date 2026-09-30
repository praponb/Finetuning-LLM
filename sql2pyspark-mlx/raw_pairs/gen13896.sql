SELECT a.id, a.opened_date, a.account_type
FROM accounts a
WHERE a.customer_id IN (SELECT id FROM customers WHERE email IS NOT NULL)
ORDER BY a.account_type
