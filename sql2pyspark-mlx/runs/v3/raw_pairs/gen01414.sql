SELECT *
FROM sessions
WHERE TO_DATE(started_at) >= '2025-03-01' AND browser = 'Safari'
ORDER BY country, customer_id;
