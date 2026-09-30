SELECT c.age, c.city, s.browser FROM customers c INNER JOIN sessions s ON s.customer_id = c.id WHERE s.customer_id IN (10, 27, 30);
