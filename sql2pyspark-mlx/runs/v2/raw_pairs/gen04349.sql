SELECT id, cost FROM shipments WHERE cost >= (SELECT MAX(cost) FROM shipments) ORDER BY id DESC;
