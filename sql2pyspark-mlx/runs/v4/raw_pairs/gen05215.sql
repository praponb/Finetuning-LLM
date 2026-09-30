SELECT s.id, s.cost, s.delivered_date FROM shipments s WHERE s.status NOT IN ('RETURNED', 'IN_TRANSIT') AND s.order_id IN (SELECT id FROM orders WHERE amount >= 1030)
