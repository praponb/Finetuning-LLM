SELECT
    o.discount,
    o.store_id,
    p.method,
    p.status
FROM orders o
LEFT JOIN payments p ON o.id = p.order_id
WHERE MONTH(o.order_date) IN (3, 6, 7) AND o.status = 'CANCELLED'
ORDER BY p.method ASC;
