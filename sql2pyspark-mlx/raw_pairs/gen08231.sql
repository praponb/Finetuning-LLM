SELECT status FROM accounts WHERE balance IS NULL EXCEPT SELECT status FROM subscriptions WHERE MONTH(start_date) IN (7, 8, 9) ORDER BY status
