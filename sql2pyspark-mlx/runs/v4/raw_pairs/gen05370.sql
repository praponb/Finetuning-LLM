SELECT
    o.id,
    o.channel,
    s.region,
    s.city,
    c.age,
    DATE_SUB(c.signup_date, 78) AS signup_date_minus_78d
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
INNER JOIN customers c ON c.id = o.customer_id
ORDER BY channel DESC;
