SELECT s.id, s.country, c.email, c.name, c.age, SUBSTRING(s.browser, 1, 2) browser_prefix FROM sessions s INNER JOIN customers c ON s.customer_id = c.id WHERE c.city = 'Singapore';
