SELECT c.city, c.email, o.discount FROM customers c JOIN orders o ON c.id = o.customer_id WHERE c.city = 'Bangkok'
