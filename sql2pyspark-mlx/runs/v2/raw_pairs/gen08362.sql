SELECT
    o.id,
    o.customer_id,
    o.store_id,
    c.signup_date,
    c.country,
    s.opened_date
FROM orders o
FULL OUTER JOIN customers c ON o.customer_id = c.id
JOIN stores s ON s.id = o.store_id
WHERE c.signup_date < '2023-04-04';
