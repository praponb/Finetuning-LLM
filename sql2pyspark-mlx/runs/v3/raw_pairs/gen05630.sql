SELECT id, signup_date FROM customers WHERE country IN ('SG', 'UK') EXCEPT SELECT id, signup_date FROM customers WHERE age >= 60 ORDER BY id
