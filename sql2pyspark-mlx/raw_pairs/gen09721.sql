SELECT * FROM (SELECT carrier, status, cost FROM shipments) PIVOT (MAX(cost) FOR status IN ('DELIVERED', 'LOST', 'RETURNED')) ORDER BY carrier;
