SELECT a.id, a.status, a.cost, a.shipped_date FROM shipments a WHERE a.cost > (SELECT AVG(b.cost) FROM shipments b WHERE b.status = a.status) ORDER BY a.status, a.shipped_date
