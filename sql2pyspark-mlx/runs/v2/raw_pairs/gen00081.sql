SELECT x.channel, y.method FROM (SELECT DISTINCT channel FROM orders) x CROSS JOIN (SELECT DISTINCT method FROM payments) y ORDER BY channel, method
