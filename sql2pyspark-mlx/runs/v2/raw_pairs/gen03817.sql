SELECT city, signup_date, CEIL(age) AS age_whole, LOWER(city) AS city_lower FROM customers WHERE credit_limit IS NULL AND email IS NOT NULL
