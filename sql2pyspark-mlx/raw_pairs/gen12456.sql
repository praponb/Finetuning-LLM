SELECT id, device, TO_DATE(started_at) AS started_at_date
FROM sessions
WHERE country = 'US' AND HOUR(started_at) BETWEEN 8 AND 12
