SELECT a.account_type, b.status FROM (SELECT DISTINCT account_type FROM accounts) a CROSS JOIN (SELECT DISTINCT status FROM subscriptions) b
