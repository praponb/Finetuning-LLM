SELECT id, name, signup_date FROM customers WHERE city = 'Bangkok' EXCEPT SELECT id, name, signup_date FROM customers WHERE country = 'JP'
