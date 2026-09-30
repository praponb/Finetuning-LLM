SELECT *
FROM (
    SELECT DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month, COUNT(CASE WHEN country = 'DE' THEN 1 END) AS de_count, MAX(signup_date) AS latest_signup_date
    FROM customers
    GROUP BY DATE_FORMAT(signup_date, 'yyyy-MM')
) AS t
WHERE de_count > 8
ORDER BY latest_signup_date DESC
