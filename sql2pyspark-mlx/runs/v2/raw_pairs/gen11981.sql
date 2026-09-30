SELECT s.id, s.customer_id, c.name, c.city, c.age FROM sessions s FULL OUTER JOIN customers c ON s.customer_id = c.id WHERE c.age BETWEEN 36 AND 61;
