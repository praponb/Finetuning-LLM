SELECT * FROM (SELECT carrier, status, cost FROM shipments WHERE cost <= 58) PIVOT (COUNT(cost) FOR status IN ('RETURNED', 'IN_TRANSIT', 'LOST', 'DELIVERED')) ORDER BY carrier;
