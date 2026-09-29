SELECT id, status FROM shipments WHERE shipped_date <= '2023-05-23' AND delivered_date > '2023-11-01' AND shipped_date BETWEEN '2023-03-21' AND '2024-04-06' ORDER BY status, id LIMIT 20;
