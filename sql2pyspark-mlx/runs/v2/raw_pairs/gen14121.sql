SELECT id, cost, status, delivered_date FROM shipments WHERE delivered_date BETWEEN '2024-03-01' AND '2024-06-17' AND shipped_date <= '2024-09-22' ORDER BY status;
