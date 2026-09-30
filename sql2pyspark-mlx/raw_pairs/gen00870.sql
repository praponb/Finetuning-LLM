SELECT c.age, o.amount FROM customers c LEFT JOIN orders o ON o.customer_id = c.id
