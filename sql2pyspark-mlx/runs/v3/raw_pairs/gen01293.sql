SELECT a.segment, b.city FROM (SELECT DISTINCT segment FROM customers) a CROSS JOIN (SELECT DISTINCT city FROM stores) b;
