SELECT a.status, b.carrier FROM (SELECT DISTINCT status FROM accounts) a CROSS JOIN (SELECT DISTINCT carrier FROM shipments) b;
