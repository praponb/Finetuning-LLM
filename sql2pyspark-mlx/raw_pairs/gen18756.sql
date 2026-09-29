SELECT start_date FROM subscriptions EXCEPT SELECT opened_date FROM accounts WHERE status NOT IN ('OPEN', 'CLOSED');
