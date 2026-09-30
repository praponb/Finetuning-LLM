SELECT name, city, CONCAT(city, ' ', email) AS city_email, TRIM(email) AS clean_email FROM customers WHERE signup_date BETWEEN '2023-08-31' AND '2025-07-01';
