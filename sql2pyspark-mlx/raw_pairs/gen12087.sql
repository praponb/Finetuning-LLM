SELECT signup_date, segment
FROM customers
WHERE country = 'TH'
UNION ALL
SELECT signup_date, segment
FROM customers
WHERE segment NOT IN ('Corporate', 'Consumer')
ORDER BY signup_date DESC
