SELECT
    o.id,
    o.amount,
    o.customer_id,
    s.city,
    s.region,
    c.signup_date
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
INNER JOIN customers c ON o.customer_id = c.id
ORDER BY o.customer_id DESC
