SELECT
    p.id,
    p.method,
    p.status,
    p.amount,
    o.status AS order_status,
    o.order_date
FROM payments p
LEFT OUTER JOIN orders o ON p.order_id = o.id
WHERE p.order_id = 25 AND o.amount < 1160
ORDER BY method DESC
