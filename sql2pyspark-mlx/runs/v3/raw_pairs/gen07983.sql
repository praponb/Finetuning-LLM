SELECT c.age, c.is_active, c.country, o.order_date FROM customers c JOIN orders o ON c.id = o.customer_id WHERE o.customer_id = 15
