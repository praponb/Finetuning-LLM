SELECT * FROM (SELECT carrier, status, cost FROM shipments) PIVOT (SUM(cost) FOR status IN ('DELIVERED', 'IN_TRANSIT', 'RETURNED', 'LOST'));
