SELECT
    o.store_id,
    o.order_date,
    p.amount,
    p.method
FROM orders o
LEFT JOIN payments p ON p.order_id = o.id
WHERE p.method IN ('promptpay', 'cash') OR p.status = 'FAILED'
ORDER BY o.order_date;
