SELECT c.id, c.segment, c.email, c.country FROM customers c WHERE c.id IN (SELECT customer_id FROM orders)
