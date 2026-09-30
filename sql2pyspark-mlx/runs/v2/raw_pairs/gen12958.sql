SELECT id, cost FROM shipments WHERE cost >= (SELECT AVG(cost) FROM shipments) ORDER BY cost DESC, id ASC LIMIT 50
