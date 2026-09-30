SELECT a.id, a.status, a.account_type
FROM accounts a
WHERE a.customer_id IS NULL AND a.customer_id IN (SELECT id FROM customers WHERE city <> 'London');
