SELECT * FROM (SELECT carrier, status, cost FROM shipments WHERE shipped_date < '2025-04-01') PIVOT (AVG(cost) FOR status IN ('RETURNED', 'IN_TRANSIT')) ORDER BY carrier;
