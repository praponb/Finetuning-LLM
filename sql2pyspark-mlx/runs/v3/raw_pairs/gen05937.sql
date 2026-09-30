SELECT
    p.id,
    p.amount,
    o.amount AS order_amount,
    o.channel,
    o.customer_id,
    TRIM(p.status) AS clean_status
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
ORDER BY o.customer_id DESC;
