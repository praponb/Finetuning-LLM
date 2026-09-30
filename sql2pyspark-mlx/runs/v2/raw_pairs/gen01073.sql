SELECT status, carrier, AVG(cost) AS avg_cost FROM shipments WHERE shipped_date > '2025-03-01' AND MONTH(shipped_date) IN (1, 8, 10) AND cost BETWEEN 59 AND 64 GROUP BY status, carrier
