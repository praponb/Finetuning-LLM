SELECT name FROM customers WHERE country IN ('JP', 'UK') UNION SELECT name FROM customers WHERE YEAR(signup_date) >= 2025
