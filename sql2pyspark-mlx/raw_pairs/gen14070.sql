SELECT
    a.id,
    a.order_id,
    a.method,
    a.status,
    b.amount AS order_amount,
    b.discount
FROM payments a
LEFT JOIN orders b
    ON a.order_id <=> b.id;
