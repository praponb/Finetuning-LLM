SELECT
    c.id,
    c.credit_limit,
    c.segment,
    c.country
FROM customers c
WHERE EXISTS (SELECT 1 FROM sessions s WHERE s.customer_id = c.id);
