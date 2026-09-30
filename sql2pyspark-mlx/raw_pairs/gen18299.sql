SELECT id, delivered_date, order_id
FROM shipments
WHERE delivered_date = (SELECT MIN(delivered_date) FROM shipments);
