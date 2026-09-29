SELECT status FROM accounts WHERE status IN ('CLOSED', 'OPEN') EXCEPT SELECT status FROM subscriptions;
