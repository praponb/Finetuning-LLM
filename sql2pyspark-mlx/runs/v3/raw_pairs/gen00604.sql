SELECT opened_date FROM accounts EXCEPT SELECT end_date FROM subscriptions WHERE status = 'ACTIVE' ORDER BY opened_date
