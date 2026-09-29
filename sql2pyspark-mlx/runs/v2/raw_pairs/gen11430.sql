SELECT id, shipped_date
FROM shipments
WHERE shipped_date > (SELECT MAX(shipped_date) FROM shipments WHERE delivered_date BETWEEN '2023-03-14' AND '2025-10-01');
