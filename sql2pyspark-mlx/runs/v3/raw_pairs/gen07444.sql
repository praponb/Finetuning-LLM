SELECT status, 'account' source FROM accounts WHERE MONTH(opened_date) IN (8, 9, 10) UNION SELECT status, 'subscription' source FROM subscriptions ORDER BY source;
