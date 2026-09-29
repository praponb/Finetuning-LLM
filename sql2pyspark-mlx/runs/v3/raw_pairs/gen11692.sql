SELECT city, MAX(signup_date) latest_signup_date
FROM customers
GROUP BY 1
ORDER BY latest_signup_date, city
LIMIT 1
