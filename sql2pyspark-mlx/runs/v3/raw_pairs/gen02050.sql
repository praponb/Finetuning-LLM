SELECT *
FROM sessions
WHERE browser = 'Chrome' AND device IN ('desktop', 'mobile')
ORDER BY browser, id
LIMIT 10
