SELECT
    o.customer_id,
    o.status,
    s.sqft,
    s.city,
    c.name,
    c.email
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
RIGHT OUTER JOIN customers c ON o.customer_id = c.id
ORDER BY o.status ASC;
