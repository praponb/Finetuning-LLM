SELECT x.country, y.region FROM (SELECT DISTINCT country FROM customers) x CROSS JOIN (SELECT DISTINCT region FROM stores) y;
