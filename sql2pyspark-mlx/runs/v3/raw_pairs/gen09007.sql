SELECT a.method, b.plan FROM (SELECT DISTINCT method FROM payments) a CROSS JOIN (SELECT DISTINCT plan FROM subscriptions) b
