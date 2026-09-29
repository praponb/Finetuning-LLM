SELECT * FROM (SELECT carrier, status, cost FROM shipments) src PIVOT (MAX(cost) FOR status IN ('IN_TRANSIT', 'DELIVERED'));
