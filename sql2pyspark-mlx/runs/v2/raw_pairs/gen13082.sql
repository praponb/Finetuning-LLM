SELECT DISTINCT browser
FROM sessions
WHERE customer_id IN (18, 35)
ORDER BY browser
