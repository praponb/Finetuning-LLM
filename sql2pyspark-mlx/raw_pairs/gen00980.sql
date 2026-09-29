SELECT x.status, y.account_type FROM (SELECT DISTINCT status FROM orders) x CROSS JOIN (SELECT DISTINCT account_type FROM accounts) y ORDER BY status, account_type
