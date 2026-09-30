SELECT *
FROM sessions
WHERE (customer_id IS NOT NULL AND TO_DATE(started_at) < '2023-03-11') OR browser = 'Chrome'
ORDER BY country DESC
