SELECT customers.id, customers.is_active
FROM customers
WHERE EXISTS (SELECT 1 FROM sessions WHERE sessions.customer_id = customers.id AND sessions.device <> 'mobile')
