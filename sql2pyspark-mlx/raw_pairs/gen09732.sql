SELECT YEAR(signup_date) AS signup_year, COUNT(age) AS age_count, MIN(credit_limit) AS min_credit_limit
FROM customers
GROUP BY signup_year
ORDER BY min_credit_limit DESC, age_count DESC, signup_year
LIMIT 10;
