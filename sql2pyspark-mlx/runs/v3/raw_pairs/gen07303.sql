SELECT s.id, s.delivered_date, o.discount FROM shipments s LEFT JOIN orders o ON s.order_id = o.id WHERE o.status IN ('CANCELLED', 'RETURNED') AND o.customer_id = 1 ORDER BY id, delivered_date;
