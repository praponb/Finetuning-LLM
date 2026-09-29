SELECT
    o.amount,
    s.store_name,
    s.sqft,
    c.country
FROM orders o
INNER JOIN stores s ON s.id = o.store_id
INNER JOIN customers c ON o.customer_id = c.id
WHERE c.segment IS NULL;
