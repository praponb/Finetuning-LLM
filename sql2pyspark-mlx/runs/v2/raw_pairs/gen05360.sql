SELECT customers.id, customers.city FROM customers WHERE customers.id IN (SELECT customer_id FROM sessions) ORDER BY customers.city;
