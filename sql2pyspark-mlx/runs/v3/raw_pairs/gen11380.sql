SELECT browser
FROM sessions
WHERE customer_id IN (5, 20, 21, 37)
INTERSECT
SELECT browser
FROM sessions
WHERE device IN ('tablet', 'desktop')
ORDER BY browser;
