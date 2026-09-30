SELECT * FROM (SELECT carrier, status, cost FROM shipments)
PIVOT (COUNT(cost) FOR status IN ('IN_TRANSIT', 'DELIVERED'))
ORDER BY carrier
