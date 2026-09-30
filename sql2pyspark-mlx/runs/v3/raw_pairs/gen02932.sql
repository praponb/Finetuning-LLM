SELECT
    p.method,
    p.amount,
    o.discount,
    o.amount AS order_amount,
    o.order_date
FROM payments p
FULL OUTER JOIN orders o ON p.order_id = o.id
WHERE o.amount <= 1400
