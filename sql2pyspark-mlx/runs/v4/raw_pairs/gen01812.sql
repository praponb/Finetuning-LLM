SELECT * FROM (SELECT region, city, sqft FROM stores WHERE sqft IS NOT NULL)
PIVOT (MAX(sqft) FOR city IN ('Chiang Mai', 'New York', 'Tokyo'))
ORDER BY region;
