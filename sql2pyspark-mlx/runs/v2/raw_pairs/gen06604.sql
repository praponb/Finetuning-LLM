SELECT
    c.id,
    c.country,
    c.segment,
    c.email
FROM customers c
WHERE LENGTH(c.email) > 7 AND NOT EXISTS (SELECT 1 FROM sessions s WHERE s.customer_id = c.id);
