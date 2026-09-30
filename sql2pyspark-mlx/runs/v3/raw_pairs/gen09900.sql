SELECT *
FROM (
    SELECT YEAR(signup_date) AS signup_year, DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month, ROUND(STDDEV(age), 2) AS stddev_age
    FROM customers
    WHERE segment IN ('Consumer', 'Corporate', 'SMB')
    GROUP BY YEAR(signup_date), DATE_FORMAT(signup_date, 'yyyy-MM')
) AS t
WHERE stddev_age >= 76
ORDER BY 2, signup_year
LIMIT 10;
