SELECT
    o.discount,
    o.customer_id,
    o.amount,
    p.paid_at,
    p.order_id,
    CAST(p.amount AS INT) AS amount_int
FROM orders o
LEFT OUTER JOIN payments p ON p.order_id = o.id
WHERE o.discount < 0.28 AND o.shipping_fee >= 28
ORDER BY customer_id DESC, paid_at;
