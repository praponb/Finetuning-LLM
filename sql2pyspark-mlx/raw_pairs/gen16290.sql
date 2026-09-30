SELECT
    a.id,
    a.customer_id,
    c.email,
    c.city
FROM accounts a
INNER JOIN customers c ON a.customer_id = c.id;
