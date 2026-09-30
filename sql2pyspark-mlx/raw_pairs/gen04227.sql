SELECT x.channel, y.account_type FROM (SELECT DISTINCT channel FROM orders) x CROSS JOIN (SELECT DISTINCT account_type FROM accounts) y ORDER BY channel, account_type;
