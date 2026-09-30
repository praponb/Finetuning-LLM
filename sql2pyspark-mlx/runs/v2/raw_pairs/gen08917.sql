SELECT * FROM (SELECT region, city, sqft FROM stores) PIVOT (AVG(sqft) FOR city IN ('Bangkok', 'New York', 'Tokyo', 'Singapore')) ORDER BY region
