SELECT status FROM accounts UNION SELECT status FROM subscriptions WHERE MONTH(end_date) IN (1, 2, 6) ORDER BY status DESC;
