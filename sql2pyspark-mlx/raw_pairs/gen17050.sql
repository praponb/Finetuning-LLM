SELECT signup_quarter, age_count
FROM (
    SELECT QUARTER(signup_date) AS signup_quarter, COUNT(age) AS age_count
    FROM customers
    GROUP BY QUARTER(signup_date)
) AS s
WHERE age_count < 7;
