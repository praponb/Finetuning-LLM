SELECT
    o.id,
    o.amount,
    s.sqft,
    c.credit_limit,
    c.country
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
RIGHT OUTER JOIN customers c ON o.customer_id = c.id
WHERE c.city IN ('Tokyo', 'Bangkok', 'Chiang Mai')
ORDER BY sqft DESC;
