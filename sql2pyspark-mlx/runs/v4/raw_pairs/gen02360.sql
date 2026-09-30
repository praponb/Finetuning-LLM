SELECT x.city, y.country FROM (SELECT DISTINCT city FROM customers) x CROSS JOIN (SELECT DISTINCT country FROM suppliers) y ORDER BY city, country;
