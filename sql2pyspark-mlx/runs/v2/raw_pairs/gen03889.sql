SELECT x.status, y.method FROM (SELECT DISTINCT status FROM orders) x CROSS JOIN (SELECT DISTINCT method FROM payments) y ORDER BY status, method
