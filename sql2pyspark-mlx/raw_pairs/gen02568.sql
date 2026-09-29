SELECT * FROM (SELECT region, city, sqft FROM stores)
PIVOT (COUNT(sqft) FOR city IN ('Chiang Mai', 'Berlin'))
