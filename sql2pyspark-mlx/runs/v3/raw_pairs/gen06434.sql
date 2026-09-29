SELECT
    o.discount,
    p.method,
    p.status,
    p.amount,
    TRIM(p.status) AS clean_status
FROM orders o
INNER JOIN payments p ON p.order_id = o.id
ORDER BY p.status DESC, method DESC
