SELECT * FROM (SELECT carrier, status, cost FROM shipments) PIVOT (SUM(cost) FOR status IN ('IN_TRANSIT', 'DELIVERED', 'RETURNED'));
