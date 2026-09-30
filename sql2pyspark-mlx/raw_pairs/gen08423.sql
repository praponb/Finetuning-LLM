SELECT a.method, b.status FROM (SELECT DISTINCT method FROM payments) a CROSS JOIN (SELECT DISTINCT status FROM subscriptions) b;
