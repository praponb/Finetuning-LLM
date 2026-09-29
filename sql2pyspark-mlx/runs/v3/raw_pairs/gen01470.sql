SELECT
    o.amount,
    o.order_date,
    o.channel,
    s.cost
FROM orders o
LEFT OUTER JOIN shipments s ON o.id = s.order_id
