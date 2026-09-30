SELECT o.amount, c.name, c.city, c.age FROM orders o LEFT JOIN customers c ON o.customer_id = c.id WHERE o.discount >= 0.2 ORDER BY age DESC
