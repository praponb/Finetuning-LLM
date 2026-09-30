SELECT x.channel, y.carrier FROM (SELECT DISTINCT channel FROM orders) x CROSS JOIN (SELECT DISTINCT carrier FROM shipments) y;
