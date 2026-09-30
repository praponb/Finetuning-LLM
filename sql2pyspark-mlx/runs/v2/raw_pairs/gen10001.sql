SELECT x.segment, y.country FROM (SELECT DISTINCT segment FROM customers) x CROSS JOIN (SELECT DISTINCT country FROM sessions) y;
