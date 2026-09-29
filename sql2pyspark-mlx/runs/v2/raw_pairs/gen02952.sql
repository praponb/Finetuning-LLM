SELECT *
FROM shipments
WHERE cost < 80 OR shipped_date BETWEEN '2023-05-01' AND '2024-08-05';
