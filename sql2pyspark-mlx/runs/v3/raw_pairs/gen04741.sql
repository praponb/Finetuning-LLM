SELECT shipments.id, shipments.carrier
FROM shipments
WHERE shipments.carrier IN ('DHL', 'Kerry') AND shipments.order_id NOT IN (SELECT id FROM orders WHERE YEAR(order_date) >= 2025)
ORDER BY carrier;
