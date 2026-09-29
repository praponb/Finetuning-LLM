SELECT id, cost, status
FROM shipments
WHERE cost >= (SELECT AVG(cost) FROM shipments);
