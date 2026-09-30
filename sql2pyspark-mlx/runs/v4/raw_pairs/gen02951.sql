SELECT opened_date FROM stores UNION SELECT start_date AS opened_date FROM subscriptions WHERE MONTH(start_date) IN (2, 6, 12);
