SELECT x.status, y.carrier FROM (SELECT DISTINCT status FROM payments) x CROSS JOIN (SELECT DISTINCT carrier FROM shipments) y
