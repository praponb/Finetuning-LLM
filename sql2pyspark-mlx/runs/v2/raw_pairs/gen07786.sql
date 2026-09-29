SELECT id, shipped_date FROM shipments WHERE shipped_date >= (SELECT MAX(shipped_date) FROM shipments WHERE delivered_date > '2023-12-25') ORDER BY shipped_date;
