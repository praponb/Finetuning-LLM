SELECT shipments.id, shipments.delivered_date, shipments.carrier
FROM shipments
WHERE shipments.cost >= 45 AND shipments.order_id IN (SELECT id FROM orders WHERE amount >= 530)
ORDER BY shipments.carrier
