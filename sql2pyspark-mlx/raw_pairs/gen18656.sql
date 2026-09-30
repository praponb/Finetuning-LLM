SELECT c.age, o.discount FROM customers c LEFT JOIN orders o ON o.customer_id = c.id WHERE o.store_id IS NOT NULL
