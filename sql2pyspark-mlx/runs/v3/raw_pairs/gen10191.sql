SELECT country, COUNT(CASE WHEN country = 'US' THEN 1 END) us_count, COUNT(credit_limit) credit_limit_count
FROM customers
WHERE signup_date >= '2023-07-13'
GROUP BY country
HAVING COUNT(CASE WHEN country = 'US' THEN 1 END) > 5
ORDER BY country, credit_limit_count DESC
