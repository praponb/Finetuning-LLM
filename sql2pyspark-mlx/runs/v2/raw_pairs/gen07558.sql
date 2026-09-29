SELECT id, carrier, shipped_date FROM (SELECT id, carrier, shipped_date, RANK() OVER (PARTITION BY carrier ORDER BY shipped_date) rnk FROM shipments) r WHERE rnk <= 5;
