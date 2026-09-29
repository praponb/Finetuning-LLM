SELECT s.id, s.browser, c.city, c.signup_date, c.name FROM sessions s JOIN customers c ON s.customer_id = c.id WHERE s.browser IN ('Safari', 'Firefox', 'Chrome') ORDER BY browser;
