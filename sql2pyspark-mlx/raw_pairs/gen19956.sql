SELECT
    s.delivered_date,
    s.status,
    o.customer_id,
    o.store_id,
    o.status AS order_status
FROM shipments s
FULL OUTER JOIN orders o ON s.order_id = o.id
WHERE o.amount > 480 AND s.order_id = 3;
