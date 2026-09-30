SELECT s.device, s.country, c.signup_date, c.credit_limit, c.country customer_country FROM sessions s RIGHT JOIN customers c ON s.customer_id = c.id WHERE s.country IN ('UK', 'US', 'DE');
