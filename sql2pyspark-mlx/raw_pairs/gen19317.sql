SELECT
    c.email,
    c.name,
    c.segment,
    s.country
FROM customers c
INNER JOIN sessions s ON c.id = s.customer_id
WHERE c.age <= 47 AND c.country != 'US';
