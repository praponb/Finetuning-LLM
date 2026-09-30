SELECT signup_month, first_signup_date, max_age
FROM (
    SELECT DATE_TRUNC('MONTH', signup_date) signup_month, MIN(signup_date) first_signup_date, MAX(age) max_age
    FROM customers
    WHERE country IN ('TH', 'UK')
    GROUP BY DATE_TRUNC('MONTH', signup_date)
) s
WHERE max_age < 50
