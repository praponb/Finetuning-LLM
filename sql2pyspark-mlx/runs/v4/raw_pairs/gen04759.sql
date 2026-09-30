SELECT
    p.amount,
    o.status,
    c.age,
    c.city
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
LEFT JOIN customers c ON o.customer_id = c.id
WHERE NOT (p.paid_at < '2024-05-03' AND HOUR(p.paid_at) BETWEEN 10 AND 18);
