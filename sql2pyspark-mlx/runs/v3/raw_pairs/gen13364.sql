SELECT a.id, a.status, a.cost FROM shipments a WHERE a.cost = (SELECT MAX(b.cost) FROM shipments b WHERE b.status = a.status) ORDER BY a.cost DESC;
