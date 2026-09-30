SELECT s.id, s.country, c.credit_limit, c.name FROM sessions s INNER JOIN customers c ON s.customer_id = c.id;
