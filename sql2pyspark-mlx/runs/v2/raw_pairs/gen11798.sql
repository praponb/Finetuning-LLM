SELECT *
FROM sessions
WHERE TO_DATE(started_at) >= '2023-09-01' AND browser = 'Edge'
