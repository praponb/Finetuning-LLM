SELECT segment, MAX(age) - MIN(age) AS age_range, MIN(credit_limit) AS min_credit_limit
FROM customers
WHERE signup_date >= '2023-07-22'
GROUP BY 1
HAVING MIN(credit_limit) >= 15600;
