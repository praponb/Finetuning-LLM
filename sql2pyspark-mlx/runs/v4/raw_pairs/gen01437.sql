SELECT x.country, y.region FROM (SELECT DISTINCT country FROM suppliers) x CROSS JOIN (SELECT DISTINCT region FROM stores) y ORDER BY country, region;
