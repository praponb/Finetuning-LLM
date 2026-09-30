SELECT browser
FROM sessions
WHERE browser IN ('Safari', 'Edge', 'Firefox')
UNION ALL
SELECT browser
FROM sessions
WHERE country = 'DE';
