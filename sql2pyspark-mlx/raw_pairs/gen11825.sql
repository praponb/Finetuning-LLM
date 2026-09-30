SELECT signup_date FROM customers WHERE signup_date IS NOT NULL INTERSECT SELECT opened_date FROM accounts WHERE opened_date IS NOT NULL
