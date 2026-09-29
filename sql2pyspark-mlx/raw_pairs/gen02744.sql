SELECT *
FROM sessions
WHERE country NOT IN ('US', 'JP', 'DE')
ORDER BY started_at
