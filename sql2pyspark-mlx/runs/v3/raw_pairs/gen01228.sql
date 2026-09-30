SELECT
    o.id,
    o.status,
    o.discount,
    s.region,
    c.age,
    c.country,
    c.age + c.credit_limit AS age_plus_credit_limit
FROM orders o
RIGHT OUTER JOIN stores s ON o.store_id = s.id
FULL OUTER JOIN customers c ON o.customer_id = c.id;
