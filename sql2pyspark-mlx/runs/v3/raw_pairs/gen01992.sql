SELECT * FROM (SELECT carrier, status, cost FROM shipments)
PIVOT (MAX(cost) FOR status IN ('IN_TRANSIT', 'LOST', 'RETURNED'))
ORDER BY carrier;
