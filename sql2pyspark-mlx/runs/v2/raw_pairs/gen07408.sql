SELECT x.status, y.plan FROM (SELECT DISTINCT status FROM accounts) x CROSS JOIN (SELECT DISTINCT plan FROM subscriptions) y
