SELECT id, cost, shipped_date FROM shipments WHERE cost > (SELECT MAX(cost) FROM shipments) ORDER BY id DESC;
