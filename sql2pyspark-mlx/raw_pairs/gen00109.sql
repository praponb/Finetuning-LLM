SELECT opened_date AS event_date FROM accounts WHERE balance BETWEEN 57000 AND 73000 UNION SELECT end_date AS event_date FROM subscriptions WHERE plan = 'standard';
