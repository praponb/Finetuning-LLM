SELECT a.country, b.city FROM (SELECT DISTINCT country FROM suppliers) a CROSS JOIN (SELECT DISTINCT city FROM stores) b ORDER BY country, city;
