SELECT s.customer_id, s.country, s.device, c.country AS customer_country FROM sessions s RIGHT JOIN customers c ON c.id = s.customer_id WHERE s.browser = 'Edge';
