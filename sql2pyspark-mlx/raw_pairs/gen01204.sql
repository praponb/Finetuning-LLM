SELECT c.id, c.credit_limit FROM customers c WHERE c.id IN (SELECT customer_id FROM accounts WHERE customer_id IS NULL)
