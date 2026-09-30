SELECT delivered_date, status FROM shipments WHERE shipped_date BETWEEN '2024-01-01' AND '2024-06-10' UNION SELECT delivered_date, status FROM shipments WHERE YEAR(shipped_date) >= 2024
