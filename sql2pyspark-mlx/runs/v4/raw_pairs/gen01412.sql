SELECT status, CAST(cost AS INT) AS cost_int
FROM shipments
WHERE shipped_date < '2023-09-14';
