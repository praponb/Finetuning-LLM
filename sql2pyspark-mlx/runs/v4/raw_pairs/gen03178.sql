SELECT a.status, b.plan FROM (SELECT DISTINCT status FROM payments) a CROSS JOIN (SELECT DISTINCT plan FROM subscriptions) b;
