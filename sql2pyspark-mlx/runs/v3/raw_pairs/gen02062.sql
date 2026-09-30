SELECT
    o.channel,
    o.status,
    o.customer_id,
    p.method,
    p.amount
FROM orders o
LEFT JOIN payments p ON p.order_id = o.id
WHERE o.amount BETWEEN 940 AND 1340
ORDER BY status DESC;
