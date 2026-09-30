SELECT id, shipped_date, carrier FROM shipments WHERE shipped_date = (SELECT MIN(shipped_date) FROM shipments WHERE cost > 76) ORDER BY id DESC
