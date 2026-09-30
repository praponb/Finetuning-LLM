SELECT s.id, s.started_at, s.browser, c.email, c.is_active FROM sessions s FULL OUTER JOIN customers c ON s.customer_id = c.id WHERE s.browser IN ('Edge', 'Chrome', 'Safari');
