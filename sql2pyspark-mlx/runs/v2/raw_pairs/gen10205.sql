SELECT customers.id, customers.segment
FROM customers
WHERE customers.id IN (SELECT customer_id FROM orders);
