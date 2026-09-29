SELECT *
FROM sessions
WHERE started_at >= '2023-03-01' AND TO_DATE(started_at) < '2024-03-15'
