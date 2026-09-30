SELECT carrier, MAX(shipped_date) AS last_shipped_date, SUM(cost) AS total_cost FROM shipments WHERE delivered_date BETWEEN '2024-07-19' AND '2024-08-01' GROUP BY carrier;
