SELECT
    o.discount,
    s.status,
    s.delivered_date,
    s.shipped_date
FROM orders o
LEFT OUTER JOIN shipments s ON s.order_id = o.id
WHERE s.order_id IS NULL;
