SELECT device, browser
FROM sessions
WHERE customer_id IN (17, 32, 34, 37)
UNION
SELECT device, browser
FROM sessions
WHERE device IN ('mobile', 'tablet')
ORDER BY browser
