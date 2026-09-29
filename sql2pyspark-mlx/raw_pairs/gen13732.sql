SELECT start_date FROM subscriptions EXCEPT SELECT signup_date FROM customers WHERE credit_limit BETWEEN 6900 AND 8700;
