SELECT
    o.id,
    o.amount,
    o.status,
    s.opened_date,
    s.sqft,
    c.signup_date,
    c.email
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
RIGHT JOIN customers c ON o.customer_id = c.id
ORDER BY sqft;
