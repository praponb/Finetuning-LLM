SELECT * FROM (SELECT region, city, sqft FROM stores WHERE region IN ('South', 'West')) PIVOT (COUNT(sqft) FOR city IN ('Singapore', 'Tokyo', 'New York')) ORDER BY region;
