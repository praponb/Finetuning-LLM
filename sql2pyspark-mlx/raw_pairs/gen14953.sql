SELECT QUARTER(signup_date) AS signup_quarter, MAX(age) - MIN(age) AS age_range
FROM customers
WHERE age BETWEEN 54 AND 56 AND (segment IN ('Corporate', 'Consumer') OR name NOT LIKE '%Ali%')
GROUP BY QUARTER(signup_date);
