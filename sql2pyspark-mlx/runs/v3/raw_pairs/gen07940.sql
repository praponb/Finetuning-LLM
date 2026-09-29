SELECT c.age, c.name, s.started_at, s.browser, CONCAT(c.city, ' - ', c.email) city_email FROM customers c LEFT JOIN sessions s ON s.customer_id = c.id ORDER BY name, city_email DESC;
