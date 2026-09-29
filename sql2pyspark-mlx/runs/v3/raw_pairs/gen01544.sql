SELECT customers.id, customers.age
FROM customers
WHERE customers.id IN (SELECT customer_id FROM accounts WHERE customer_id IS NULL)
