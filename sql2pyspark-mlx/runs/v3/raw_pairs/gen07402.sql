SELECT browser, country
FROM sessions
WHERE customer_id IN (5, 17)
UNION ALL
SELECT browser, country
FROM sessions
WHERE country != 'TH'
ORDER BY country;
