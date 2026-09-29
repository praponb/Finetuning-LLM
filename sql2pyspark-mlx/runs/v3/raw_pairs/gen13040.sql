SELECT * FROM (SELECT carrier, status, cost FROM shipments WHERE status IN ('RETURNED', 'DELIVERED', 'LOST')) PIVOT (MAX(cost) FOR status IN ('RETURNED', 'IN_TRANSIT', 'DELIVERED'))
