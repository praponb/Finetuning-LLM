SELECT id, shipped_date, delivered_date FROM shipments WHERE delivered_date BETWEEN '2024-01-01' AND '2024-02-01' ORDER BY shipped_date, id LIMIT 5
