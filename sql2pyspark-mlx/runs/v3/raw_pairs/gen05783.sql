SELECT a.channel, b.plan FROM (SELECT DISTINCT channel FROM orders) a CROSS JOIN (SELECT DISTINCT plan FROM subscriptions) b;
