SELECT
    o.order_date,
    o.status,
    o.discount,
    p.amount,
    p.order_id,
    p.method
FROM orders o
LEFT OUTER JOIN payments p ON p.order_id = o.id
WHERE o.customer_id IN (14, 16)
