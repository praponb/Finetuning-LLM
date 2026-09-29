SELECT status, 'account' AS source FROM accounts WHERE account_type IN ('SAVINGS', 'LOAN') UNION SELECT status, 'subscription' AS source FROM subscriptions;
