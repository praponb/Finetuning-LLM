SELECT id, delivered_date, cost FROM shipments WHERE delivered_date = (SELECT MIN(delivered_date) FROM shipments) ORDER BY cost, delivered_date;
