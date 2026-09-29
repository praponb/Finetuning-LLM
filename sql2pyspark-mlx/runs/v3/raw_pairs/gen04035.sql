SELECT id, delivered_date, status, cost FROM shipments WHERE delivered_date = (SELECT MIN(delivered_date) FROM shipments) ORDER BY cost DESC;
