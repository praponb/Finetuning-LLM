SELECT is_active, city, MAX(signup_date) AS latest_signup_date FROM customers WHERE age BETWEEN 43 AND 66 GROUP BY is_active, city ORDER BY city;
