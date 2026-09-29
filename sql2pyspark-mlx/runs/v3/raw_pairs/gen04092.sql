SELECT x.segment, y.region FROM (SELECT DISTINCT segment FROM customers) x CROSS JOIN (SELECT DISTINCT region FROM stores) y ORDER BY segment, region;
