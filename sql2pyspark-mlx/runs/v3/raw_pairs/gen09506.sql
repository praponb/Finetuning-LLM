SELECT c.id, c.age, c.segment FROM customers c WHERE c.email IS NOT NULL AND c.id IN (SELECT customer_id FROM accounts)
