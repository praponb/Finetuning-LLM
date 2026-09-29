SELECT * FROM (SELECT carrier, status, cost FROM shipments)
PIVOT (AVG(cost) FOR status IN ('LOST', 'IN_TRANSIT', 'DELIVERED'));
