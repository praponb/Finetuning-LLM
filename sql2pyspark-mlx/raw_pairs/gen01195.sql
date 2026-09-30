SELECT * FROM (SELECT status, carrier, cost FROM shipments WHERE carrier = 'Flash') PIVOT (AVG(cost) FOR carrier IN ('Kerry', 'Flash', 'UPS', 'FedEx')) ORDER BY status;
