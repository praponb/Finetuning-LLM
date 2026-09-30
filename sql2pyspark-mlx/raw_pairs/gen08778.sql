SELECT x.segment, y.city FROM (SELECT DISTINCT segment FROM customers) x CROSS JOIN (SELECT DISTINCT city FROM stores) y ORDER BY segment, city
