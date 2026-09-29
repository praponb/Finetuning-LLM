SELECT id, cost, carrier, status FROM shipments WHERE cost < (SELECT MIN(cost) FROM shipments);
