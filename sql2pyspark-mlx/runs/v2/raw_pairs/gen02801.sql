SELECT id, delivered_date, order_id
FROM shipments
WHERE carrier IS NOT NULL;
