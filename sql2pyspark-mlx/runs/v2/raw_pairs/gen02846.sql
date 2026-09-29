SELECT * FROM (SELECT carrier, status, cost FROM shipments WHERE carrier = 'FedEx')
PIVOT (AVG(cost) FOR status IN ('DELIVERED', 'RETURNED', 'IN_TRANSIT', 'LOST'));
