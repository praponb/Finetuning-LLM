SELECT * FROM (SELECT carrier, status, cost FROM shipments WHERE cost <= 49) PIVOT (AVG(cost) FOR status IN ('RETURNED', 'DELIVERED', 'IN_TRANSIT', 'LOST'));
