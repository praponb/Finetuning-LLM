SELECT * FROM (SELECT carrier, status, cost FROM shipments) PIVOT (AVG(cost) FOR status IN ('IN_TRANSIT', 'LOST', 'RETURNED'))
