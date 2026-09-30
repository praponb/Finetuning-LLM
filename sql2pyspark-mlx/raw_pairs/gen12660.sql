SELECT
    c.id,
    c.segment,
    c.email,
    c.name
FROM customers c
WHERE EXISTS (SELECT 1 FROM sessions s WHERE s.customer_id = c.id AND s.device IN ('mobile', 'desktop'));
