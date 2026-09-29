SELECT browser
FROM sessions
WHERE browser = 'Chrome'
UNION ALL
SELECT browser
FROM sessions
WHERE country = 'JP';
