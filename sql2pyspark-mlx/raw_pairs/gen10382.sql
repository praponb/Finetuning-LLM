SELECT id, order_id, cost, ROUND(cost * 100, 2) AS adjusted_cost FROM shipments WHERE status = 'LOST' AND (status = 'IN_TRANSIT' OR status = 'IN_TRANSIT');
