SELECT *
FROM (
    SELECT DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month, MIN(signup_date) AS min_signup_date, COUNT(*) AS total_count
    FROM customers
    GROUP BY DATE_FORMAT(signup_date, 'yyyy-MM')
) AS x
WHERE total_count >= 7
ORDER BY signup_month
