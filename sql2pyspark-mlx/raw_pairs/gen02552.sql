SELECT MAX(credit_limit) AS max_credit_limit, ROUND(STDDEV(age), 2) AS stddev_age FROM customers WHERE email LIKE '%@gmail.com' ORDER BY stddev_age;
