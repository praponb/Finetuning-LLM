SELECT signup_date, name FROM customers WHERE country IN ('SG', 'TH', 'DE') UNION SELECT signup_date, name FROM customers WHERE age >= 56 ORDER BY name
