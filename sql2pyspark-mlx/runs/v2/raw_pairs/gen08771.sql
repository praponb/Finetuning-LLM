SELECT id, country
FROM sessions
WHERE browser = 'Firefox'
UNION ALL
SELECT id, country
FROM sessions
WHERE country IS NULL
