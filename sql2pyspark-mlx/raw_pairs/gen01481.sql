SELECT carrier, delivered_date FROM shipments WHERE NOT (shipped_date BETWEEN '2023-09-01' AND '2025-04-01' AND shipped_date > '2024-01-08');
