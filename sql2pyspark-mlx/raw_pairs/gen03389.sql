SELECT
    c.id,
    c.name,
    c.country,
    c.signup_date
FROM customers c
WHERE c.city = 'Bangkok' AND EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id AND a.status != 'OPEN');
