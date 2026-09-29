SELECT
    o.discount,
    o.customer_id,
    p.order_id,
    p.amount,
    p.status,
    CONCAT(o.channel, '/', o.status) AS channel_status
FROM orders o
LEFT OUTER JOIN payments p ON o.id = p.order_id
WHERE NOT (p.amount >= 810 AND o.shipping_fee BETWEEN 34 AND 40);
