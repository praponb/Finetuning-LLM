SELECT
    p.id,
    p.amount,
    p.status,
    p.method
FROM payments p
WHERE p.order_id IN (SELECT id FROM orders WHERE MONTH(order_date) IN (3, 9, 11));
