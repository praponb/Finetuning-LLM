SELECT id, cost, status, carrier, TRIM(carrier) AS clean_carrier FROM shipments WHERE YEAR(shipped_date) = 2025 AND delivered_date BETWEEN '2024-02-01' AND '2024-11-12';
