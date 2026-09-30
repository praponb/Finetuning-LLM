SELECT id, delivered_date FROM shipments WHERE delivered_date = (SELECT MAX(delivered_date) FROM shipments) ORDER BY id;
