SELECT *
FROM sessions
WHERE customer_id IS NULL AND country NOT IN ('JP', 'DE')
