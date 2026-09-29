SELECT country, browser
FROM sessions
WHERE country = 'TH'
UNION ALL
SELECT country, browser
FROM sessions
WHERE device IN ('mobile', 'tablet', 'desktop')
