SELECT
    o.store_id,
    o.customer_id,
    o.discount,
    p.amount,
    p.order_id,
    p.paid_at
FROM orders o
LEFT JOIN payments p ON o.id = p.order_id;
