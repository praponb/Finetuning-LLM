SELECT x.country, y.city FROM (SELECT DISTINCT country FROM suppliers) x CROSS JOIN (SELECT DISTINCT city FROM stores) y ORDER BY country, city;
