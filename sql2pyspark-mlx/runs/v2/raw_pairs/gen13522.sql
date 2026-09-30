SELECT x.account_type, y.method FROM (SELECT DISTINCT account_type FROM accounts) x CROSS JOIN (SELECT DISTINCT method FROM payments) y ORDER BY account_type, method;
