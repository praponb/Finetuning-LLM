SELECT customers.id, customers.is_active FROM customers WHERE customers.id IN (SELECT customer_id FROM accounts);
