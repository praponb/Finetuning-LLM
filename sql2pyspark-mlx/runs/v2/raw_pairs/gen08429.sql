SELECT * FROM (SELECT carrier, status, cost FROM shipments)
PIVOT (SUM(cost) FOR status IN ('LOST', 'IN_TRANSIT', 'RETURNED', 'DELIVERED'))
ORDER BY carrier
