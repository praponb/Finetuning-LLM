SELECT id, cost FROM shipments WHERE cost > (SELECT AVG(cost) FROM shipments WHERE carrier = 'Kerry')
