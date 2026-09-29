SELECT a.id, a.account_type
FROM accounts a
WHERE a.customer_id IN (SELECT id FROM customers WHERE country NOT IN ('SG', 'UK'))
ORDER BY account_type
