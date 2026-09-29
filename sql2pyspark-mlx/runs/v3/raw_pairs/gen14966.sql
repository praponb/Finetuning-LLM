SELECT
    o.channel,
    o.status,
    p.status AS payment_status,
    p.method
FROM orders o
LEFT JOIN payments p ON o.id = p.order_id
WHERE o.channel IN ('store', 'mobile')
ORDER BY channel DESC;
