SELECT segment, signup_date FROM customers WHERE city = 'Bangkok' EXCEPT SELECT segment, signup_date FROM customers WHERE UPPER(TRIM(email)) = 'FRANK@COMPANY.CO'
