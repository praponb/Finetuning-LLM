SELECT
    o.customer_id,
    s.sqft,
    c.signup_date,
    c.age
FROM orders o
JOIN stores s ON o.store_id = s.id
JOIN customers c ON c.id = o.customer_id
WHERE c.segment IS NOT NULL AND o.amount BETWEEN 1280 AND 1350
