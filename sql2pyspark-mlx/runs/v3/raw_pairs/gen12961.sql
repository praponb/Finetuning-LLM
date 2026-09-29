SELECT id, start_date FROM subscriptions WHERE start_date = (SELECT MIN(start_date) FROM subscriptions);
