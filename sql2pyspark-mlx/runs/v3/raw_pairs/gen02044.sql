SELECT
    o.customer_id,
    o.amount,
    s.carrier,
    s.shipped_date,
    s.delivered_date
FROM orders o
LEFT OUTER JOIN shipments s ON s.order_id = o.id
ORDER BY s.shipped_date ASC;
