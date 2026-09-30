SELECT cost, carrier, delivered_date
FROM shipments
WHERE status IN ('IN_TRANSIT', 'LOST');
