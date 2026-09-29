SELECT id, age, country, signup_date FROM customers WHERE LENGTH(email) < 8 OR segment != 'SMB' ORDER BY signup_date
