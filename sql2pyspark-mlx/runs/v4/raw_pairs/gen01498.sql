SELECT signup_date event_date FROM customers EXCEPT SELECT start_date event_date FROM subscriptions WHERE start_date < '2023-05-01';
