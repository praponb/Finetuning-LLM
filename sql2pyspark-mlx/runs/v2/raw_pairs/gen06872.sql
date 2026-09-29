SELECT
    o.order_date,
    c.signup_date,
    c.credit_limit,
    s.sqft,
    s.store_name
FROM orders o
RIGHT JOIN customers c ON c.id = o.customer_id
INNER JOIN stores s ON o.store_id = s.id;
