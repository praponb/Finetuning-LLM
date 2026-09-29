SELECT o.id, o.amount FROM orders o WHERE o.customer_id IN (SELECT id FROM customers WHERE age BETWEEN 40 AND 67) ORDER BY o.amount DESC;
