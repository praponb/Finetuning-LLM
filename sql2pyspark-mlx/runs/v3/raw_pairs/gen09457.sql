SELECT
    o.id,
    o.store_id,
    o.status,
    o.customer_id,
    c.city,
    CONCAT(c.country, '/', c.email) AS country_email
FROM orders o
LEFT OUTER JOIN customers c ON o.customer_id = c.id
