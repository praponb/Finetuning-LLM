SELECT * FROM shipments WHERE MONTH(shipped_date) IN (8, 9, 11) AND carrier = 'UPS' ORDER BY delivered_date DESC, 6 DESC, id ASC LIMIT 5;
