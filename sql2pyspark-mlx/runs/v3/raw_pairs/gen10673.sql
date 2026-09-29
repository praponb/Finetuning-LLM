SELECT
    o.id,
    o.discount,
    c.credit_limit,
    c.age - c.credit_limit AS age_minus_credit_limit
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
WHERE o.amount > 970
