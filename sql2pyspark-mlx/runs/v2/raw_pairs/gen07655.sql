SELECT c.id, c.segment, c.is_active, c.country FROM customers c WHERE c.id IN (SELECT customer_id FROM accounts)
