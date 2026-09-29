SELECT *
FROM (
    SELECT QUARTER(signup_date) AS signup_quarter, ROUND(STDDEV(age), 2) AS stddev_age, MIN(age) AS min_age
    FROM customers
    GROUP BY QUARTER(signup_date)
) AS sub
WHERE min_age > 54
ORDER BY signup_quarter
LIMIT 5;
