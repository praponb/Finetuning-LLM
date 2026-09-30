SELECT o.id, o.store_id, o.discount FROM orders o WHERE o.customer_id NOT IN (SELECT id FROM customers WHERE name LIKE '%Emm%');
