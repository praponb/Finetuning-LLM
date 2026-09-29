SELECT a.status, b.account_type FROM (SELECT DISTINCT status FROM orders) a CROSS JOIN (SELECT DISTINCT account_type FROM accounts) b ORDER BY status, account_type;
