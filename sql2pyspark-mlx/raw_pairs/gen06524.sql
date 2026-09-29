SELECT c.id, c.signup_date, c.country
FROM customers c
WHERE LOWER(c.email) LIKE '%vy@%' AND NOT EXISTS (SELECT 1 FROM sessions s WHERE s.customer_id = c.id AND s.browser IN ('Safari', 'Firefox', 'Chrome'));
