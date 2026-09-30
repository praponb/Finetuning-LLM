SELECT signup_date FROM customers WHERE credit_limit > 11900 UNION ALL SELECT signup_date FROM customers WHERE city = 'Chiang Mai' ORDER BY signup_date;
