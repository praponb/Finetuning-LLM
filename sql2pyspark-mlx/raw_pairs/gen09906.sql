SELECT id, signup_date FROM customers WHERE signup_date = (SELECT MAX(signup_date) FROM customers);
