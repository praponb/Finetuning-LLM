SELECT x.country, y.city FROM (SELECT DISTINCT country FROM customers) x CROSS JOIN (SELECT DISTINCT city FROM suppliers) y;
