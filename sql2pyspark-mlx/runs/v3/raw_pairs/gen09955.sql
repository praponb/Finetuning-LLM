WITH ranked AS (SELECT *, DENSE_RANK() OVER (PARTITION BY device ORDER BY started_at) AS pos FROM sessions WHERE started_at < '2025-01-01') SELECT * FROM ranked WHERE pos = 1
