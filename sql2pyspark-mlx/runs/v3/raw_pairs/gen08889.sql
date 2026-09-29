SELECT o.amount, s.carrier, s.shipped_date
FROM orders o
LEFT JOIN shipments s ON s.order_id = o.id
