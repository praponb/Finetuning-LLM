SELECT
    shipments.id,
    shipments.cost,
    shipments.order_id,
    shipments.status
FROM shipments
WHERE shipments.order_id IN (1, 15, 31, 33) AND shipments.order_id IN (SELECT id FROM orders WHERE discount <= 0.38)
ORDER BY shipments.order_id DESC
