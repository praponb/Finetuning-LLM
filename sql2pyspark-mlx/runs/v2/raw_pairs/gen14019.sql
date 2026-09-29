SELECT s.browser, c.name, c.age FROM sessions s RIGHT JOIN customers c ON s.customer_id = c.id WHERE s.browser != 'Chrome';
