SELECT shipments.id, shipments.shipped_date, shipments.status
FROM shipments
WHERE shipments.order_id NOT IN (SELECT id FROM orders WHERE amount < 960);
