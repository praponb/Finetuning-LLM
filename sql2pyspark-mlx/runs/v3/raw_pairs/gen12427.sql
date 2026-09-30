SELECT x.method, y.plan FROM (SELECT DISTINCT method FROM payments) x CROSS JOIN (SELECT DISTINCT plan FROM subscriptions) y
