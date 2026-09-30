SELECT
    o.channel,
    o.order_date,
    o.discount,
    p.amount,
    p.paid_at,
    p.method
FROM orders o
LEFT JOIN payments p ON p.order_id = o.id;
