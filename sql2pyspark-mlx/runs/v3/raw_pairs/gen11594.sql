SELECT DATE_TRUNC('MONTH', signup_date) AS signup_month, YEAR(signup_date) AS signup_year, ROUND((100 * SUM(CASE WHEN city = 'Chiang Mai' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_chiang_mai
FROM customers
WHERE name LIKE '%Kan%' AND age BETWEEN 42 AND 61
GROUP BY signup_month, signup_year
HAVING ROUND((100 * SUM(CASE WHEN city = 'Chiang Mai' THEN 1 ELSE 0 END)) / COUNT(*), 1) < 55
