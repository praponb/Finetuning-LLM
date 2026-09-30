SELECT * FROM (SELECT region, city, sqft FROM stores) PIVOT (AVG(sqft) FOR city IN ('New York', 'Chiang Mai', 'Singapore', 'Tokyo')) ORDER BY region;
