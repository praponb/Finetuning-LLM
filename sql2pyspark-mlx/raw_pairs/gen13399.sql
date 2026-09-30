SELECT
    p.id,
    p.amount,
    o.status,
    c.is_active,
    c.email,
    TO_DATE(p.paid_at) AS paid_at_date
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
INNER JOIN customers c ON c.id = o.customer_id
