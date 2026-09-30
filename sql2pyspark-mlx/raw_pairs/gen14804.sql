SELECT
    o.id,
    o.customer_id,
    o.amount,
    c.signup_date,
    c.city,
    s.store_name
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
RIGHT JOIN stores s ON o.store_id = s.id
WHERE c.credit_limit BETWEEN 7700 AND 9600
ORDER BY c.signup_date DESC;
