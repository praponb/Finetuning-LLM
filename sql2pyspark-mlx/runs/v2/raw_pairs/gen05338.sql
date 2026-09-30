SELECT signup_month, de_count, pct_corporate
FROM (
    SELECT DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month, COUNT(CASE WHEN country = 'DE' THEN 1 END) AS de_count, ROUND((100 * SUM(CASE WHEN segment = 'Corporate' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_corporate
    FROM customers
    GROUP BY DATE_FORMAT(signup_date, 'yyyy-MM')
) AS t
WHERE de_count > 9
ORDER BY de_count, signup_month;
