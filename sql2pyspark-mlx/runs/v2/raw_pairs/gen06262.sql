SELECT x.channel, y.plan FROM (SELECT DISTINCT channel FROM orders) x CROSS JOIN (SELECT DISTINCT plan FROM subscriptions) y
