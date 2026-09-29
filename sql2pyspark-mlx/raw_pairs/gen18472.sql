SELECT city, country FROM customers WHERE signup_date > '2023-05-19' UNION SELECT city, country FROM customers WHERE city != 'Bangkok' ORDER BY city;
