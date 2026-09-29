SELECT start_date FROM subscriptions WHERE MONTH(start_date) IN (4, 6, 11) EXCEPT SELECT launch_date AS start_date FROM products ORDER BY start_date;
