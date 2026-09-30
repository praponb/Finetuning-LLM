SELECT YEAR(signup_date) AS signup_year, SUM(CASE WHEN city = 'Berlin' THEN 1 ELSE 0 END) AS berlin_count
FROM customers
GROUP BY YEAR(signup_date)
