SELECT QUARTER(signup_date) AS signup_quarter, MAX(age) AS max_age, MAX(age) - MIN(age) AS age_range
FROM customers
GROUP BY QUARTER(signup_date)
ORDER BY signup_quarter DESC, max_age DESC;
