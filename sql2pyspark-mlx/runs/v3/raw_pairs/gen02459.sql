SELECT
    s.id,
    s.delivered_date,
    s.status,
    o.amount,
    o.channel,
    o.order_date
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
WHERE o.customer_id IS NULL AND s.shipped_date < '2023-07-16';
