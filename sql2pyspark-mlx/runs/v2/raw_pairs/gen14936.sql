SELECT * FROM (SELECT region, city, sqft FROM stores) PIVOT (COUNT(sqft) FOR city IN ('Bangkok', 'New York', 'Tokyo', 'Chiang Mai'));
