SELECT shipments.id, shipments.status FROM shipments WHERE shipments.delivered_date <= '2025-04-19' AND shipments.order_id NOT IN (SELECT id FROM orders WHERE amount >= 960);
