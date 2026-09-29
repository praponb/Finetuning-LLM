SELECT s.country, s.customer_id, s.browser, c.segment, c.is_active FROM sessions s JOIN customers c ON s.customer_id = c.id WHERE s.country IN ('US', 'JP') OR c.country = 'DE';
